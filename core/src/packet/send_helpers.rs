use std::io::{Cursor, Error, ErrorKind};

use binrw::BinWrite;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{common::timestamp_msecs, config::get_config, ipc::kawari::CustomIpcSegment};

use super::{
    CompressionType, ConnectionState, ConnectionType, PacketHeader, PacketSegment,
    ReadWriteIpcSegment, SegmentData, SegmentType, compression::compress, parse_packet,
    parse_packet_header,
};

pub async fn send_packet<T: ReadWriteIpcSegment>(
    socket: &mut TcpStream,
    state: &mut ConnectionState,
    connection_type: ConnectionType,
    compression_type: CompressionType,
    segments: &[PacketSegment<T>],
) {
    let (data, uncompressed_size) = compress(state, &compression_type, segments);
    let size = std::mem::size_of::<PacketHeader>() + data.len();

    let header = PacketHeader {
        timestamp: timestamp_msecs(),
        size: size as u32,
        connection_type,
        segment_count: segments.len() as u16,
        compression_type,
        uncompressed_size: uncompressed_size as u32,
        ..Default::default()
    };

    let mut cursor = Cursor::new(Vec::with_capacity(size));
    header.write_le(&mut cursor).unwrap();
    std::io::Write::write_all(&mut cursor, &data).unwrap();

    let buffer = cursor.into_inner();

    if let Err(e) = socket.write_all(&buffer).await {
        tracing::warn!("Failed to send packet: {e}");
    }
}

pub async fn read_packet(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let header_size = std::mem::size_of::<PacketHeader>();
    let mut packet_buffer = vec![0; header_size];
    socket.read_exact(&mut packet_buffer).await?;

    let header = parse_packet_header(&packet_buffer);
    let packet_size = header.size as usize;
    if packet_size < header_size {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("invalid packet size {}", header.size),
        ));
    }

    packet_buffer.resize(packet_size, 0);
    socket.read_exact(&mut packet_buffer[header_size..]).await?;

    Ok(packet_buffer)
}

pub async fn send_keep_alive<T: ReadWriteIpcSegment>(
    socket: &mut TcpStream,
    state: &mut ConnectionState,
    connection_type: ConnectionType,
    id: u32,
    timestamp: u32,
) {
    let response_packet: PacketSegment<T> = PacketSegment {
        segment_type: SegmentType::KeepAliveResponse,
        data: SegmentData::KeepAliveResponse { id, timestamp },
        ..Default::default()
    };
    send_packet(
        socket,
        state,
        connection_type,
        CompressionType::Uncompressed,
        &[response_packet],
    )
    .await;
}

/// Sends a custom IPC packet to the world server, meant for private server-to-server communication.
/// Returns the first custom IPC segment returned.
pub async fn send_custom_world_packet(segment: CustomIpcSegment) -> Option<CustomIpcSegment> {
    let config = get_config();

    let addr = config.world.get_public_socketaddr();

    let mut stream = TcpStream::connect(addr).await.ok()?;

    let mut packet_state = ConnectionState::None;

    let segment: PacketSegment<CustomIpcSegment> = PacketSegment {
        segment_type: SegmentType::KawariIpc,
        data: SegmentData::KawariIpc(segment),
        ..Default::default()
    };

    send_packet(
        &mut stream,
        &mut packet_state,
        ConnectionType::KawariIpc,
        CompressionType::Uncompressed,
        &[segment],
    )
    .await;

    let packet_buffer = read_packet(&mut stream).await.ok()?;
    let segments = parse_packet::<CustomIpcSegment>(&packet_buffer, &mut packet_state);

    if let Some(segment) = segments.first() {
        return match &segment.data {
            SegmentData::KawariIpc(data) => Some(data.clone()),
            _ => None,
        };
    }

    None
}
