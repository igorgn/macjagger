//! Agent library code.
//!
//! TODO: Implement this as you progress through stages.

use std::{time::Duration};

use std::process::Stdio;
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use tokio::{io::{AsyncBufReadExt, BufReader}, process::{Command}, sync::mpsc, time::interval};
use tokio_tungstenite::tungstenite::Message;

use crate::{HealthResponse, Job, RegisterAgentRequest, RegisterAgentResponse};
/// Run the CI agent, connecting to the server at the given URL.
/// TODO Stage 1: Ping server health endpoint.
/// TODO Stage 2: Register and heartbeat.
/// TODO Stage 3: Concurrent heartbeat + work loop.
/// TODO Stage 4: Stream logs via WebSocket.
/// TODO Stage 5: Respect backpressure and retry with backoff.
/// TODO Stage 6: Poll for jobs, execute them, report status.
async fn get_agent_hostname() -> Result<String> {
    let hostname = Command::new("hostname").output().await?;
    let str_hostname = String::from_utf8_lossy(&hostname.stdout.trim_ascii()).to_string();
    Ok(str_hostname)
}

async fn heartbeat(request_client: &Client, heartbeat_url: &String) -> Result<()> {
    let resp = request_client.post(heartbeat_url).send().await?;
    match resp.status() {
        StatusCode::OK => println!("heartbeat ok"),
        StatusCode::NOT_FOUND => println!("unexpected status"),
        _ => println!("unexpected status"),
    }

    Ok(())
}

pub async fn run(server_url: String) -> Result<()> {
    let healthcheck_url = format!("{}/health", &server_url);
    let register_url = format!("{}/agents", &server_url);
    let request_client = Client::new();

    let server_health: HealthResponse = request_client
        .get(healthcheck_url)
        .send()
        .await?
        .json()
        .await?;

    assert_eq!(server_health.status, "ok");

    let agent = RegisterAgentRequest {
        name: get_agent_hostname().await?
    };

    let register_response = request_client
        .post(&register_url)
        .json(&agent)
        .send()
        .await?;

    let status_code = register_response.status();
    let agent_response: RegisterAgentResponse = register_response.json().await?;
    assert_eq!(status_code, StatusCode::CREATED);

    let agent_id = agent_response.id;

    println!("Id: {}", agent_id);

    let ws_url = format!(
        "{}/agents/{}/ws",
        server_url.replace("http://", "ws://"),
        agent_id
    );

    println!("WS connect to {}", ws_url);
    let (socket, _) = tokio_tungstenite::connect_async(ws_url).await?;
    let (mut writer, mut reader) = socket.split();

    tokio::spawn(async move {
        while let Some(Ok(msg)) = reader.next().await {
            let job: Job = serde_json::from_str(&msg.to_string()).unwrap();
            println!("got job: {}", job.id);
            match execute_job(&mut writer, job).await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            }
        };
    });
    
    let mut interval = interval(Duration::from_secs(5));
    let heartbeat_url = format!("{}/agents/{}/heartbeat", &server_url, agent_id);
    loop {
        tokio::select! {
            biased;
            _ = interval.tick() => {
                heartbeat(&request_client, &heartbeat_url).await?

            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shuttind down gracefully...");
                break
            }
        }
    }
    Ok(())
}

async fn execute_job(
    ws_writer: &mut futures::prelude::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Message,
    >,
    job: Job,
) -> Result<()> {

    let (tx, mut rx) = mpsc::channel::<String>(100);
    let message = format!("Job {} started!", job.id);
    ws_writer.send(Message::Text(message)).await.unwrap();
    let script = format!("set -e\n{}", job.cmd);
    let mut cmd = Command::new("sh")
        .arg("-c")
        .arg(script)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
   let stdout = cmd.stdout.take().unwrap(); 
   let stderr = cmd.stderr.take().unwrap(); 
   let log_reader = BufReader::new(stdout);
   let err_reader = BufReader::new(stderr);

   let tx_logs = tx.clone();
   tokio::spawn(async move {
       let mut log_lines = log_reader.lines();
       while let Ok(Some(log_line)) = log_lines.next_line().await {
          tx_logs.send(log_line).await.expect("Error while sending log to mpsc channel"); 
      }
   });
   
   let tx = tx.clone();
   tokio::spawn(async move {
       let mut err_lines = err_reader.lines();
       while let Ok(Some(log_line)) = err_lines.next_line().await {
          tx.send(log_line).await.expect("Error while sending log to mpsc channel"); 
      }
   });

       while let Some(log) = rx.recv().await {
          ws_writer.send(Message::Text(log)).await?; 
       }
       let exit_status = cmd.wait().await?;
       println!("Exit status: {}", exit_status);
       ws_writer.send(Message::Text(exit_status.to_string())).await?;
    Ok(())
}

// async fn read_stdout(mut stdout_handle: Option<ChildStdout>) -> String { while let Some(log_line) = stdout_handle.take() { log_line.read_to_string(&mut buf); } } async fn read_stderr(mut stderr_handle: Option<ChildStderr>) { }