use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::BrokerError;

pub async fn read_frame<T: AsyncReadExt + Unpin>(reader: &mut T) -> Result<Vec<u8>, BrokerError> {
    let mut header = [0u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|err| BrokerError::Ipc(err.to_string()))?;
    let size = u32::from_be_bytes(header) as usize;
    let mut payload = vec![0u8; size];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|err| BrokerError::Ipc(err.to_string()))?;
    Ok(payload)
}

pub async fn write_frame<T: AsyncWriteExt + Unpin>(
    writer: &mut T,
    payload: &[u8],
) -> Result<(), BrokerError> {
    let size = payload.len() as u32;
    writer
        .write_all(&size.to_be_bytes())
        .await
        .map_err(|err| BrokerError::Ipc(err.to_string()))?;
    writer
        .write_all(payload)
        .await
        .map_err(|err| BrokerError::Ipc(err.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|err| BrokerError::Ipc(err.to_string()))?;
    Ok(())
}
