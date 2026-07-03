//! Shared types and modules for the CI system.
//!
//! This file is pre-filled with types that both server and agent use.
//! As you progress through stages, you'll use more of these types.

pub mod agent;
pub mod server;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::response::IntoResponse;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

// ============================================================
// Stage 1: Basic JSON types
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

impl Default for HealthResponse {
    fn default() -> Self {
        Self {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

// ============================================================
// Stage 2: Agent types
// ============================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    acknowledged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentInfo {
    pub id: String,
    pub name: String,
    pub status: AgentStatus,
    pub last_heartbeat: u64,
    pub assigned_job: Option<Job>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentStatus {
    Idle,
    Busy,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegisterAgentRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegisterAgentResponse {
    pub id: String,
    pub message: String,
}

#[derive(Clone)]
pub struct AppState {
    agents: Arc<RwLock<HashMap<String, AgentInfo>>>,
    agent_sockets: Arc<RwLock<HashMap<String, Sender<Job>>>>,
    jobs: Arc<RwLock<HashMap<String, Job>>>,
    job_sender: Sender<String>,
}

impl AppState {
    pub fn new(tx: Sender<String>) -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
            agent_sockets: Arc::new(RwLock::new(HashMap::new())),
            jobs: Arc::new(RwLock::new(HashMap::new())),
            job_sender: tx,
        }
    }
}
// ============================================================
// Stage 3-6: Job types
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub id: String,
    pub repo_url: String,
    pub branch: String,
    pub cmd: String,
    pub status: JobStatus,
    pub assigned_agent: Option<String>,
}

impl From<CreateJobRequest> for Job {
    fn from(req: CreateJobRequest) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            repo_url: req.repo_url,
            branch: req.branch,
            cmd: req.cmd,
            status: JobStatus::Queued,
            assigned_agent: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Queued,
    Running,
    Success,
    Failed,
    Cancelled,
}

// pub struct JobSteps {
//     steps: Vec<String>
// }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateJobRequest {
    pub repo_url: String,
    pub branch: String,
    pub cmd: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobResponse {
    pub id: String,
    pub status: JobStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub timestamp: u64,
    pub line: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NextJobResponse {
    pub has_job: bool,
    pub job: Option<Job>,
}

pub fn timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug)]
pub enum AppError {
    Conflict,
    Internal,
    NotFound,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::Conflict => (StatusCode::CONFLICT).into_response(),
            AppError::Internal => (StatusCode::INTERNAL_SERVER_ERROR).into_response(),
            AppError::NotFound => (StatusCode::NOT_FOUND).into_response(),
        }
    }
}

pub struct ServerGuard(pub tokio::process::Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.start_kill().ok();
    }
}
