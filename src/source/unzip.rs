//! Streaming decompress: lê um ZIP de uma `AsyncRead`, devolve outra `AsyncRead`
//! com o conteúdo descomprimido da primeira entrada (todo ZIP da Receita tem
//! uma única entrada CSV). A descompressão acontece numa task spawnada que
//! escreve num `DuplexStream`.

use anyhow::{anyhow, Result};
use async_zip::base::read::stream::ZipFileReader;
use tokio::io::{AsyncBufRead, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;
use tokio_util::compat::FuturesAsyncReadCompatExt;

const PIPE_BUF: usize = 256 * 1024;

pub fn decompress_first_entry<R>(zip_reader: R) -> (DuplexStream, JoinHandle<Result<()>>)
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    let (mut writer, reader_out) = tokio::io::duplex(PIPE_BUF);

    let task = tokio::spawn(async move {
        let zip = ZipFileReader::with_tokio(zip_reader);
        let Some(mut reading) = zip.next_with_entry().await? else {
            return Err(anyhow!("zip vazio"));
        };

        {
            let mut compat = reading.reader_mut().compat();
            tokio::io::copy(&mut compat, &mut writer).await?;
        }
        writer.shutdown().await?;
        reading.done().await?;
        Ok(())
    });

    (reader_out, task)
}
