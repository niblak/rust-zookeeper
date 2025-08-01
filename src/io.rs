use consts::{ZkError, ZkState};
use listeners::ListenerSet;
use proto::{
    ByteBuf, ConnectRequest, ConnectResponse, OpCode, ReadFrom, ReplyHeader, RequestHeader, WriteTo,
};
use watch::WatchMessage;
use zookeeper::{RawRequest, RawResponse};

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token, Waker};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::io;
use std::io::{Cursor, ErrorKind};
use std::mem;
use std::net::SocketAddr;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ZK: Token = Token(1);
const TIMER: Token = Token(2);
const CHANNEL: Token = Token(3);

use try_io::{TryRead, TryWrite};

lazy_static! {
    static ref PING: ByteBuf = RequestHeader {
        xid: -2,
        opcode: OpCode::Ping
    }
    .to_len_prefixed_buf()
    .unwrap();
}

// Custom timer implementation to replace mio-extras timer
#[derive(Debug, Clone)]
struct TimeoutEntry<T> {
    deadline: Instant,
    id: usize,
    data: T,
}

impl<T> PartialEq for TimeoutEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}

impl<T> Eq for TimeoutEntry<T> {}

impl<T> PartialOrd for TimeoutEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for TimeoutEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (earliest deadline first)
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.id.cmp(&self.id))
    }
}

#[derive(Debug)]
struct SimpleTimeout {
    id: usize,
}

#[derive(Debug)]
struct SimpleTimer<T> {
    heap: BinaryHeap<TimeoutEntry<T>>,
    next_id: usize,
}

impl<T> SimpleTimer<T> {
    fn new() -> Self {
        SimpleTimer {
            heap: BinaryHeap::new(),
            next_id: 0,
        }
    }

    fn set_timeout(&mut self, duration: Duration, data: T) -> SimpleTimeout {
        let deadline = Instant::now() + duration;
        let id = self.next_id;
        self.next_id += 1;

        self.heap.push(TimeoutEntry { deadline, id, data });

        SimpleTimeout { id }
    }

    fn cancel_timeout(&mut self, timeout: &SimpleTimeout) {
        // Mark as cancelled by removing from heap
        // Note: This is a simplified implementation. In practice, we'd want to mark
        // entries as cancelled rather than rebuilding the heap, but for this use case
        // it's sufficient since timeouts are infrequent.
        self.heap.retain(|entry| entry.id != timeout.id);
    }

    fn poll(&mut self) -> Option<T> {
        let now = Instant::now();
        while let Some(entry) = self.heap.peek() {
            if entry.deadline <= now {
                let entry = self.heap.pop().unwrap();
                return Some(entry.data);
            } else {
                break;
            }
        }
        None
    }

    fn next_timeout(&self) -> Option<Duration> {
        self.heap.peek().map(|entry| {
            let now = Instant::now();
            if entry.deadline > now {
                entry.deadline - now
            } else {
                Duration::from_millis(0)
            }
        })
    }
}

// Custom channel implementation to replace mio-extras channel
struct MioChannel<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    waker: Arc<Waker>,
}

fn mio_channel<T>(poll: &Poll) -> io::Result<MioChannel<T>> {
    let (sender, receiver) = mpsc::channel();
    let waker = Arc::new(Waker::new(poll.registry(), CHANNEL)?);

    Ok(MioChannel {
        sender,
        receiver,
        waker,
    })
}

impl<T> MioChannel<T> {
    fn sender(&self) -> MioChannelSender<T> {
        MioChannelSender {
            sender: self.sender.clone(),
            waker: Arc::clone(&self.waker),
        }
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone)]
pub struct MioChannelSender<T> {
    sender: Sender<T>,
    waker: Arc<Waker>,
}

impl<T> MioChannelSender<T> {
    pub fn send(&self, msg: T) -> Result<(), mpsc::SendError<T>> {
        let result = self.sender.send(msg);
        if result.is_ok() {
            // Wake up the event loop
            let _ = self.waker.wake();
        }
        result
    }
}

struct Hosts {
    addrs: Vec<SocketAddr>,
    index: usize,
}

impl Hosts {
    fn new(addrs: Vec<SocketAddr>) -> Hosts {
        Hosts {
            addrs: addrs,
            index: 0,
        }
    }

    fn get(&mut self) -> &SocketAddr {
        let addr = &self.addrs[self.index];
        if self.addrs.len() == self.index + 1 {
            self.index = 0;
        } else {
            self.index += 1;
        }
        addr
    }
}

#[derive(Clone, Debug)]
enum ZkTimeout {
    Ping,
    Connect,
}

pub struct ZkIo {
    sock: TcpStream,
    state: ZkState,
    hosts: Hosts,
    buffer: VecDeque<RawRequest>,
    inflight: VecDeque<RawRequest>,
    response: BytesMut,
    ping_timeout: Option<SimpleTimeout>,
    conn_timeout: Option<SimpleTimeout>,
    timer: SimpleTimer<ZkTimeout>,
    timeout_ms: u64,
    ping_timeout_duration: Duration,
    conn_timeout_duration: Duration,
    watch_sender: mpsc::Sender<WatchMessage>,
    conn_resp: ConnectResponse,
    zxid: i64,
    ping_sent: Instant,
    state_listeners: ListenerSet<ZkState>,
    poll: Poll,
    shutdown: bool,
    tx: MioChannelSender<RawRequest>,
    rx: MioChannel<RawRequest>,
}

impl ZkIo {
    pub fn new(
        addrs: Vec<SocketAddr>,
        ping_timeout_duration: Duration,
        watch_sender: mpsc::Sender<WatchMessage>,
        state_listeners: ListenerSet<ZkState>,
    ) -> ZkIo {
        trace!("ZkIo::new");
        let timeout_ms = ping_timeout_duration.as_secs() * 1000
            + ping_timeout_duration.subsec_nanos() as u64 / 1000000;

        // TODO I need a socket here, sorry.
        let sock = TcpStream::connect(addrs[0]).unwrap();

        // TODO add error handling to this method in subsequent commit.
        // There's already another unwrap which needs to be addressed.
        let poll = Poll::new().unwrap();
        let channel = mio_channel(&poll).unwrap();
        let tx = channel.sender();

        let mut zkio = ZkIo {
            sock,
            state: ZkState::Connecting,
            hosts: Hosts::new(addrs),
            buffer: VecDeque::new(),
            inflight: VecDeque::new(),
            // TODO server reads max up to 1MB, otherwise drops the connection,
            // size should be 1MB + tcp rcvBufsize
            response: BytesMut::with_capacity(1024 * 1024 * 2),
            ping_timeout: None,
            conn_timeout: None,
            ping_timeout_duration: ping_timeout_duration,
            conn_timeout_duration: Duration::from_secs(2),
            timeout_ms: timeout_ms,
            watch_sender: watch_sender,
            conn_resp: ConnectResponse::initial(timeout_ms),
            zxid: 0,
            ping_sent: Instant::now(),
            state_listeners: state_listeners,
            poll,
            shutdown: false,
            timer: SimpleTimer::new(),
            tx,
            rx: channel,
        };

        let request = zkio.connect_request();
        zkio.buffer.push_back(request);
        zkio
    }

    fn reregister(&mut self, interest: Interest) {
        self.poll
            .registry()
            .reregister(&mut self.sock, ZK, interest)
            .expect("Failed to register ZK handle");
    }

    fn notify_state(&self, old_state: ZkState, new_state: ZkState) {
        if new_state != old_state {
            self.state_listeners.notify(&new_state);
        }
    }

    fn handle_response(&mut self) {
        loop {
            if self.response.len() <= 4 {
                return;
            }

            let len = BigEndian::read_i32(&self.response[..4]) as usize;

            trace!(
                "Response chunk len = {} buf len is {}",
                len,
                self.response.len()
            );

            if self.response.len() - 4 < len {
                return;
            } else {
                self.response.advance(4);
                let bytes = self.response.split_to(len);
                self.handle_chunk(bytes.freeze());

                self.response.reserve(1024 * 1024 * 2);
            }
        }
    }

    fn handle_chunk(&mut self, bytes: Bytes) {
        let len = bytes.len();
        trace!("handle_response in {:?} state [{}]", self.state, len);

        let mut data = &*bytes;

        if self.state != ZkState::Connecting {
            let header = match ReplyHeader::read_from(&mut data) {
                Ok(header) => header,
                Err(e) => {
                    warn!("Failed to parse ReplyHeader {:?}", e);
                    self.inflight.pop_front();
                    return;
                }
            };
            if header.zxid > 0 {
                // Update last-seen zxid when this is a request response
                self.zxid = header.zxid;
            }
            let response = RawResponse {
                header: header,
                data: Cursor::new(data.bytes().to_vec()),
            }; // TODO COPY!
            match response.header.xid {
                -1 => {
                    trace!("handle_response Got a watch event!");
                    self.watch_sender
                        .send(WatchMessage::Event(response))
                        .unwrap();
                }
                -2 => {
                    trace!("Got ping response in {:?}", self.ping_sent.elapsed());
                    self.inflight.pop_front();
                }
                _ => match self.inflight.pop_front() {
                    Some(request) => {
                        if request.opcode == OpCode::CloseSession {
                            let old_state = self.state;
                            self.state = ZkState::Closed;
                            self.notify_state(old_state, self.state);
                            self.shutdown = true;
                        }
                        self.send_response(request, response);
                    }
                    None => panic!("Shouldn't happen, no inflight request"),
                },
            }
        } else {
            self.inflight.pop_front(); // drop the connect request

            let conn_resp = match ConnectResponse::read_from(&mut data) {
                Ok(conn_resp) => conn_resp,
                Err(e) => {
                    panic!("Failed to parse ConnectResponse {:?}", e);
                    // self.reconnect();
                    // return
                }
            };

            let old_state = self.state;

            if conn_resp.timeout == 0 {
                info!("session {} expired", self.conn_resp.session_id);
                self.conn_resp.session_id = 0;
                self.state = ZkState::NotConnected;
            } else {
                self.conn_resp = conn_resp;
                trace!("Connected: {:?}", self.conn_resp);
                self.timeout_ms = self.conn_resp.timeout;
                self.ping_timeout_duration = Duration::from_millis(self.conn_resp.timeout / 3 * 2);

                self.state = if self.conn_resp.read_only {
                    ZkState::ConnectedReadOnly
                } else {
                    ZkState::Connected
                };
            }

            self.notify_state(old_state, self.state);
        }
    }

    fn send_response(&self, request: RawRequest, response: RawResponse) {
        match request.listener {
            Some(ref listener) => {
                trace!("send_response Opcode is {:?}", request.opcode);
                listener.send(response).unwrap();
            }
            None => info!("Nobody is interested in response {:?}", request.opcode),
        }
        match (request.opcode, request.watch) {
            (OpCode::RemoveWatches, Some(w)) => self
                .watch_sender
                .send(WatchMessage::RemoveWatch(w.path, w.watcher_type))
                .unwrap(),
            (_, Some(w)) => self.watch_sender.send(WatchMessage::Watch(w)).unwrap(),
            (_, None) => {}
        }
    }

    fn clear_timeout(&mut self, atype: ZkTimeout) {
        let timeout = match atype {
            ZkTimeout::Ping => mem::replace(&mut self.ping_timeout, None),
            ZkTimeout::Connect => mem::replace(&mut self.conn_timeout, None),
        };
        if let Some(timeout) = timeout {
            trace!("clear_timeout: {:?}", atype);
            self.timer.cancel_timeout(&timeout);
        }
    }

    fn start_timeout(&mut self, atype: ZkTimeout) {
        self.clear_timeout(atype.clone());
        trace!("start_timeout: {:?}", atype);
        match atype {
            ZkTimeout::Ping => {
                let duration = self.ping_timeout_duration.clone();
                self.ping_timeout = Some(self.timer.set_timeout(duration, atype));
            }
            ZkTimeout::Connect => {
                let duration = self.conn_timeout_duration.clone();
                self.conn_timeout = Some(self.timer.set_timeout(duration, atype));
            }
        }
        // No need to reregister timer in mio 1.0 - we'll handle timeouts in poll loop
    }

    fn reconnect(&mut self) {
        trace!("reconnect");
        let old_state = self.state;
        self.state = ZkState::Connecting;
        self.notify_state(old_state, self.state);

        info!("Establishing Zk connection");

        // TODO only until session times out
        loop {
            self.buffer.clear();
            self.inflight.clear();
            self.response.clear(); // TODO drop all read bytes once RingBuf.clear() is merged

            // Check if the session is still alive according to our knowledge
            if self.ping_sent.elapsed().as_secs() * 1000 > self.timeout_ms {
                warn!("Zk session timeout, closing io event loop");
                self.state = ZkState::Closed;
                self.notify_state(ZkState::Connecting, self.state);
                self.shutdown = true;
                break;
            }

            self.clear_timeout(ZkTimeout::Ping);
            self.clear_timeout(ZkTimeout::Connect);
            {
                let host = self.hosts.get();
                info!("Connecting to new server {:?}", host);
                self.sock = match TcpStream::connect(*host) {
                    Ok(sock) => sock,
                    Err(e) => {
                        error!("Failed to connect {:?}: {:?}", host, e);
                        continue;
                    }
                };
                info!("Started connecting to {:?}", host);
            }
            self.start_timeout(ZkTimeout::Connect);

            let request = self.connect_request();
            self.buffer.push_back(request);

            // Register the new socket
            self.poll
                .registry()
                .register(&mut self.sock, ZK, Interest::READABLE | Interest::WRITABLE)
                .expect("Register ZK");

            break;
        }
    }

    fn connect_request(&self) -> RawRequest {
        let conn_req = ConnectRequest::from(&self.conn_resp, self.zxid);
        let buf = conn_req.to_len_prefixed_buf().unwrap();
        RawRequest {
            opcode: OpCode::Auth,
            data: buf,
            listener: None,
            watch: None,
        }
    }

    fn ready(&mut self, token: Token, event: &mio::event::Event) {
        trace!(
            "event token={:?} readable={} writable={}",
            token,
            event.is_readable(),
            event.is_writable()
        );

        match token {
            ZK => self.ready_zk(event),
            TIMER => self.ready_timer(),
            CHANNEL => self.ready_channel(),
            _ => unreachable!(),
        }
    }

    fn ready_zk(&mut self, event: &mio::event::Event) {
        self.clear_timeout(ZkTimeout::Ping);

        if event.is_writable() {
            while let Some(mut request) = self.buffer.pop_front() {
                match self.sock.try_write_buf(&mut request.data) {
                    Ok(Some(0)) => {
                        warn!("Connection closed: write");
                        self.reconnect();
                        return;
                    }
                    Ok(Some(written)) => {
                        trace!("Written {:?} bytes", written);
                        if request.data.has_remaining() {
                            self.buffer.push_front(request);
                            break;
                        } else {
                            self.inflight.push_back(request);
                        }
                    }
                    Ok(None) => trace!("Spurious write"),
                    Err(e) => match e.kind() {
                        ErrorKind::WouldBlock => {
                            trace!("Got WouldBlock IO Error, no need to reconnect.")
                        }
                        _ => {
                            error!("Failed to write socket: {:?}", e);
                            self.reconnect();
                            return;
                        }
                    },
                }
            }
        }

        if event.is_readable() {
            match self.sock.try_read_buf(&mut self.response) {
                Ok(Some(0)) => {
                    warn!("Connection closed: read");
                    self.reconnect();
                    return;
                }
                Ok(Some(read)) => {
                    trace!("Read {:?} bytes", read);
                    self.handle_response();
                }
                Ok(None) => trace!("Spurious read"),
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        trace!("Got WouldBlock IO Error, no need to reconnect.")
                    }
                    _ => {
                        error!("Failed to read socket: {:?}", e);
                        self.reconnect();
                        return;
                    }
                },
            }
        }

        if event.is_error() && (self.state != ZkState::Closed) {
            // If we were connected
            // fn send_watched_event(keeper_state: KeeperState) {
            //     match sender.send(WatchedEvent{event_type: WatchedEventType::None,
            //                                    keeper_state: keeper_state,
            //                                    path: None}) {
            //         Ok(()) => (),
            //         Err(e) => panic!("Reader/Writer: Event died {}", e)
            //     }
            // }
            let old_state = self.state;
            self.state = ZkState::NotConnected;
            self.notify_state(old_state, self.state);

            info!("Reconnect due to HUP");
            self.reconnect();
        }

        if self.is_idle() {
            self.start_timeout(ZkTimeout::Ping);
        }

        // Not sure that we need to write, but we always need to read, because of watches
        // If the output buffer has no content, we don't need to write again
        let mut interest = Interest::READABLE | Interest::WRITABLE;
        if self.buffer.is_empty() {
            interest = Interest::READABLE;
        }

        // This tick is done, subscribe to a forthcoming one
        self.reregister(interest);
    }

    fn is_idle(&self) -> bool {
        self.inflight.is_empty() && self.buffer.is_empty()
    }

    fn ready_channel(&mut self) {
        while let Ok(request) = self.rx.try_recv() {
            trace!("ready_channel {:?}", request.opcode);

            match self.state {
                ZkState::Closed => {
                    // If zk is unavailable, respond with a ConnectionLoss error.
                    let header = ReplyHeader {
                        xid: 0,
                        zxid: 0,
                        err: ZkError::ConnectionLoss as i32,
                    };
                    let response = RawResponse {
                        header: header,
                        data: ByteBuf::new(vec![]),
                    };
                    self.send_response(request, response);
                }
                _ => {
                    // Otherwise, queue request for processing.
                    if self.buffer.is_empty() {
                        self.reregister(Interest::READABLE | Interest::WRITABLE);
                    }
                    self.buffer.push_back(request);
                }
            }
        }
        // No need to reregister channel in mio 1.0 - waker handles notifications
    }

    fn ready_timer(&mut self) {
        trace!("ready_timer thread={:?}", ::std::thread::current().id());

        loop {
            match self.timer.poll() {
                Some(ZkTimeout::Ping) => {
                    trace!("handle ping timeout");
                    self.clear_timeout(ZkTimeout::Ping);
                    if self.inflight.is_empty() {
                        // No inflight request indicates an idle connection. Send a ping.
                        trace!("Pinging {:?}", self.sock.peer_addr().unwrap());
                        self.tx
                            .send(RawRequest {
                                opcode: OpCode::Ping,
                                data: PING.clone(),
                                listener: None,
                                watch: None,
                            })
                            .unwrap();
                        self.ping_sent = Instant::now();
                    }
                }
                Some(ZkTimeout::Connect) => {
                    trace!("handle connection timeout");
                    self.clear_timeout(ZkTimeout::Connect);
                    if self.state == ZkState::Connecting {
                        info!("Reconnect due to connection timeout");
                        self.reconnect();
                    }
                }
                None => {
                    break;
                }
            }
        }
    }

    pub fn sender(&self) -> MioChannelSender<RawRequest> {
        MioChannelSender {
            sender: self.tx.sender.clone(),
            waker: Arc::clone(&self.tx.waker),
        }
    }

    pub fn run(mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(128);

        // Register Initial Interest
        self.poll.registry().register(
            &mut self.sock,
            ZK,
            Interest::READABLE | Interest::WRITABLE,
        )?;

        loop {
            // Handle loop shutdown
            if self.shutdown {
                break;
            }

            // Calculate timeout for next timer event
            let timeout = self.timer.next_timeout();

            // Wait for events
            self.poll.poll(&mut events, timeout)?;

            // Handle timer events first
            self.ready_timer();

            // Handle channel events
            self.ready_channel();

            // Process socket events
            for event in &events {
                if event.token() == ZK {
                    self.ready(event.token(), event);
                }
            }
        }

        Ok(())
    }
}
