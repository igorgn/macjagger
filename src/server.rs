//! Server library code.
//!
//! TODO: Implement this as you progress through stages.
use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};

use std::net::SocketAddr;
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, Receiver},
};

use crate::{
    timestamp_now, AgentInfo, AgentStatus, AppError, AppState, CreateJobRequest, HealthResponse,
    HeartbeatResponse, Job, JobResponse, JobStatus, RegisterAgentRequest, RegisterAgentResponse,
};
/// Run the CI server on the given address.
/// TODO Stage 1: Implement basic health endpoint.
/// TODO Stage 2: Add agent registry state.
/// TODO Stage 3: Add job queue and background processor.
/// TODO Stage 4: Add WebSocket endpoint for logs.
/// TODO Stage 5: Add backpressure (Semaphore, bounded channels).
/// TODO Stage 6: Add job dispatch logic.

pub async fn run(addr: SocketAddr) {
    let (tx, rx) = mpsc::channel::<String>(10);
    let state = AppState::new(tx);
    let listener = TcpListener::bind(addr).await.unwrap();
    tokio::spawn(dispatcher(rx, state.clone()));
    let app = Router::new()
        .route("/", get(homepage))
        .route("/health", get(healthcheck))
        .route("/agents", get(get_agents).post(register_agent))
        .route("/jobs", get(get_jobs).post(register_job))
        .route("/jobs/:id/logs", get(register_job))
        .route("/agents/:id/heartbeat", post(agent_heartbeat))
        .route("/agents/:id/ws", get(agent_ws_handler))
        .with_state(state);

    axum::serve(listener, app).await.unwrap();
}

async fn agent_ws_handler(
    Path(agent_id): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    if !state.agents.read().unwrap().contains_key(&agent_id) {
        return (StatusCode::NOT_FOUND, "Agent {} is not found").into_response();
    }
    ws.on_upgrade(move |socket| socket_handler(socket, agent_id, state))
}

async fn socket_handler(socket: WebSocket, agent_id: String, state: AppState) {
    let (mut ws_sender, mut ws_reciever) = socket.split();
    let (job_sender, mut job_reciever) = mpsc::channel::<Job>(32);
    state
        .agent_sockets
        .write()
        .unwrap()
        .entry(agent_id.clone())
        .or_insert(job_sender);

    println!("ws agent connected: {}", agent_id);
    tokio::spawn(async move {
        while let Some(job) = job_reciever.recv().await {
            println!("ws job recieved: {}", job.id);
            ws_sender
                .send(Message::Text(serde_json::to_string(&job).unwrap()))
                .await
                .unwrap();
        }
    });

    while let Some(msg) = ws_reciever.next().await {
        if let Ok(msg) = msg {
            println!("Agent {}: {}", agent_id, msg.to_text().unwrap())
        }
    }
}

async fn dispatcher(mut rx: Receiver<String>, state: AppState) -> Result<()> {
    while let Some(job_id) = rx.recv().await {
        println!("got job_id: {}", job_id);
        assign_job_to_agent(job_id, state.clone()).await.unwrap();
    }
    Ok(())
}

async fn assign_job_to_agent(job_id: String, state: AppState) -> Result<(), AppError> {
    let free_agent_id = {
        state
            .agents
            .read()
            .unwrap()
            .iter()
            .find(|(_, a)| a.status == AgentStatus::Idle)
            .map(|(id, _)| id.clone())
    };

    let Some(free_agent_id) = free_agent_id else {
        return Ok(());
    };

    if !state.jobs.read().unwrap().contains_key(&job_id) {
        return Err(AppError::NotFound);
    }

    let job = {
        let mut jobs = state.jobs.write().unwrap();
        let job = jobs.get_mut(&job_id).unwrap();
        job.assigned_agent = Some(free_agent_id.clone());
        job.status = JobStatus::Running;
        job.clone()
    };

    {
        let mut agents = state.agents.write().unwrap();
        let free_agent = agents.get_mut(&free_agent_id).unwrap();
        free_agent.status = AgentStatus::Busy;
        free_agent.assigned_job = Some(job.clone());
    }

    let tx = state
        .agent_sockets
        .read()
        .unwrap()
        .get(&free_agent_id)
        .cloned()
        .unwrap();
    tx.send(job).await.unwrap();

    // let mut tx;
    // let mut agents = state.agents.write().unwrap();
    // if let Some((agent_id, agent)) = agents
    //     .iter_mut()
    //     .find(|(_, agent)| agent.status == AgentStatus::Idle)
    // {
    //     agent.status = crate::AgentStatus::Busy;
    //     if let Some(job) = state.jobs.write().unwrap().get_mut(&job_id) {
    //         job.status = JobStatus::Running;
    //         job.assigned_agent = Some(agent_id.clone());
    //         agent.assigned_job = Some(job.clone());
    //         tx = state.agent_sockets.read().unwrap().get(agent_id).cloned();
    //             // tx.send(job.clone()).await;
    //     }
    // }
    Ok(())
}

async fn agent_heartbeat(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<HeartbeatResponse>, StatusCode> {
    state
        .agents
        .write()
        .unwrap()
        .get_mut(&id)
        .map(|agent| {
            agent.last_heartbeat = timestamp_now();
            Json(HeartbeatResponse {
                acknowledged: true,
                // assigned_job: agent.assigned_job.clone(),
            })
        })
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_jobs(State(state): State<AppState>) -> Json<Vec<Job>> {
    let j: Vec<Job> = state.jobs.read().unwrap().values().cloned().collect();
    Json(j)
}

async fn register_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> (StatusCode, Json<JobResponse>) {
    let job = Job::from(req);
    let job_id = job.id.clone();
    println!("job: {:?}; job_id: {}", job, job_id);
    println!("build steps: {}", job.cmd);
    state.jobs.write().unwrap().insert(job_id.clone(), job);
    state.job_sender.send(job_id.clone()).await.unwrap();

    (
        StatusCode::CREATED,
        Json(JobResponse {
            id: job_id,
            status: JobStatus::Queued,
        }),
    )
}

async fn get_agents(State(state): State<AppState>) -> Json<Vec<AgentInfo>> {
    let agents: Vec<AgentInfo> = state.agents.read().unwrap().values().cloned().collect();
    Json(agents)
}
async fn healthcheck() -> Json<HealthResponse> {
    Json(HealthResponse::default())
}

async fn homepage() -> Html<String> {
    Html("ok".into())
}

async fn register_agent(
    State(state): State<AppState>,
    req: Json<RegisterAgentRequest>,
) -> (StatusCode, Json<RegisterAgentResponse>) {
    let id = format!("agent-{}", &req.name);
    let mut message = "acknowledged";
    let timestamp = timestamp_now();
    let mut agents = state.agents.write().unwrap();
    agents
        .entry(id.clone())
        .and_modify(|agent| {
            agent.last_heartbeat = timestamp;
            message = "heartbeat"
        })
        .or_insert(AgentInfo {
            id: id.clone(),
            name: req.name.clone(),
            status: crate::AgentStatus::Idle,
            last_heartbeat: timestamp,
            assigned_job: None,
        });
    (
        StatusCode::CREATED,
        Json(RegisterAgentResponse {
            id,
            message: message.to_string(),
        }),
    )
}
