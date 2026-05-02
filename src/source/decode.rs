//! Decoder streaming: ISO-8859-1 / WINDOWS-1252 → UTF-8. A Receita publica os
//! CSVs em latin-1, mas as ferramentas modernas (csv-async, sqlx) esperam UTF-8.

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;

const PIPE_BUF: usize = 256 * 1024;
const READ_BUF: usize = 16 * 1024;

pub fn latin1_to_utf8<R>(input: R) -> (DuplexStream, JoinHandle<Result<()>>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (mut writer, reader_out) = tokio::io::duplex(PIPE_BUF);

    let task = tokio::spawn(async move {
        let mut input = input;
        let mut decoder = encoding_rs::WINDOWS_1252.new_decoder_without_bom_handling();
        let mut in_buf = vec![0u8; READ_BUF];
        let mut out_buf = String::with_capacity(READ_BUF * 2);

        loop {
            let n = input.read(&mut in_buf).await?;
            if n == 0 {
                out_buf.clear();
                let (_, _, _had_errors) = decoder.decode_to_string(&[], &mut out_buf, true);
                if !out_buf.is_empty() {
                    writer.write_all(out_buf.as_bytes()).await?;
                }
                break;
            }
            out_buf.clear();
            let (_, _, _had_errors) = decoder.decode_to_string(&in_buf[..n], &mut out_buf, false);
            writer.write_all(out_buf.as_bytes()).await?;
        }
        writer.shutdown().await?;
        Ok(())
    });

    (reader_out, task)
}
