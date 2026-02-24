use anyhow::{anyhow, Context, Result};
use p2p_dfs_node::config::{
    Cli, ClientAddArgs, ClientBaseArgs, ClientCancelDownloadArgs, ClientDownloadStatusArgs,
    ClientGetArgs, ClientProvideArgs, Command, DaemonArgs,
};
use p2p_dfs_node::control_plane::DfsControlService;
use p2p_dfs_node::grpc_api::dfs::control::v1::dfs_control_client::DfsControlClient;
use p2p_dfs_node::grpc_api::dfs::control::v1::dfs_control_server::DfsControlServer;
use p2p_dfs_node::grpc_api::dfs::control::v1::{
    AddFileRequest, CancelDownloadRequest, DownloadStatusRequest, Empty, GetFileRequest,
    ProvideRequest,
};
use p2p_dfs_node::node::start_node;
use tokio::signal;
use tonic::transport::Server;
use tracing::{debug, info};

fn init_logging(filter: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    debug!("logging initialized with filter={filter}");
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse_args();

    match cli.command {
        Command::Daemon(args) => run_daemon(args).await,
        Command::Add(args) => run_add(args).await,
        Command::Provide(args) => run_provide(args).await,
        Command::Providing(args) => run_providing(args).await,
        Command::Get(args) => run_get(args).await,
        Command::List(args) => run_list(args).await,
        Command::Status(args) => run_status(args).await,
        Command::DownloadStatus(args) => run_download_status(args).await,
        Command::CancelDownload(args) => run_cancel_download(args).await,
        Command::Peers(args) => run_peers(args).await,
    }
}

async fn run_daemon(args: DaemonArgs) -> Result<()> {
    args.validate()?;
    init_logging(&args.log_filter());

    let (handle, mut runtime_task) = start_node(args.clone()).await?;
    let service = DfsControlService::new(handle.client());
    let mut grpc_shutdown_rx = handle.shutdown_receiver();

    let grpc_addr = args.grpc_addr;
    let grpc_server = Server::builder()
        .add_service(DfsControlServer::new(service))
        .serve_with_shutdown(grpc_addr, async move {
            loop {
                if *grpc_shutdown_rx.borrow() {
                    break;
                }

                if grpc_shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        });

    let mut grpc_task = tokio::spawn(async move {
        grpc_server
            .await
            .with_context(|| format!("gRPC server failed on {grpc_addr}"))
    });

    info!(
        "daemon started: p2p_listen={} grpc_addr={}",
        args.listen_p2p, args.grpc_addr
    );

    let mut runtime_result: Option<Result<()>> = None;
    let mut grpc_result: Option<Result<()>> = None;

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("received Ctrl+C, shutting down daemon");
        }
        result = &mut runtime_task => {
            runtime_result = Some(
                result
                    .map_err(|error| anyhow!("runtime task join error: {error}"))?
            );
        }
        result = &mut grpc_task => {
            grpc_result = Some(
                result
                    .map_err(|error| anyhow!("gRPC task join error: {error}"))?
            );
        }
    }

    handle.shutdown()?;

    if runtime_result.is_none() {
        runtime_result = Some(
            runtime_task
                .await
                .map_err(|error| anyhow!("runtime task join error: {error}"))?,
        );
    }

    if grpc_result.is_none() {
        grpc_result = Some(
            grpc_task
                .await
                .map_err(|error| anyhow!("gRPC task join error: {error}"))?,
        );
    }

    if let Some(result) = grpc_result {
        result?;
    }

    if let Some(result) = runtime_result {
        result?;
    }

    info!("daemon shutdown complete");
    Ok(())
}

async fn run_add(args: ClientAddArgs) -> Result<()> {
    let mut client = connect_client(&args.base).await?;
    let response = client
        .add_file(AddFileRequest {
            path: args.path.display().to_string(),
            public: args.public,
        })
        .await?
        .into_inner();

    println!("{}", response.cid);
    Ok(())
}

async fn run_provide(args: ClientProvideArgs) -> Result<()> {
    let mut client = connect_client(&args.base).await?;
    client
        .provide(ProvideRequest { cid: args.cid })
        .await
        .map_err(|error| anyhow!("Provide request failed: {error}"))?;

    println!("accepted");
    Ok(())
}

async fn run_get(args: ClientGetArgs) -> Result<()> {
    let mut client = connect_client(&args.base).await?;
    client
        .get_file(GetFileRequest {
            cid: args.cid,
            output_path: args.output.display().to_string(),
        })
        .await
        .map_err(|error| anyhow!("GetFile request failed: {error}"))?;

    println!("accepted");
    Ok(())
}

async fn run_providing(args: ClientBaseArgs) -> Result<()> {
    let mut client = connect_client(&args).await?;
    let response = client
        .list_providing(Empty {})
        .await
        .map_err(|error| anyhow!("ListProviding request failed: {error}"))?
        .into_inner();

    for cid in response.cids {
        println!("{cid}");
    }

    Ok(())
}

async fn run_list(args: ClientBaseArgs) -> Result<()> {
    let mut client = connect_client(&args).await?;
    let response = client
        .list_local(Empty {})
        .await
        .map_err(|error| anyhow!("ListLocal request failed: {error}"))?
        .into_inner();

    for cid in response.cids {
        println!("{cid}");
    }

    Ok(())
}

async fn run_status(args: ClientBaseArgs) -> Result<()> {
    let mut client = connect_client(&args).await?;
    let response = client
        .status(Empty {})
        .await
        .map_err(|error| anyhow!("Status request failed: {error}"))?
        .into_inner();

    println!("peer_id: {}", response.peer_id);
    println!("listen_addrs: {}", response.listen_addrs.join(", "));
    println!("connected_peers: {}", response.connected_peers);
    println!("known_peers: {}", response.known_peers);
    println!("mdns_enabled: {}", response.mdns_enabled);
    println!("announcements_enabled: {}", response.announcements_enabled);
    println!("local_file_count: {}", response.local_file_count);
    println!("providing_count: {}", response.providing_count);
    println!("active_downloads: {}", response.active_downloads);

    Ok(())
}

async fn run_download_status(args: ClientDownloadStatusArgs) -> Result<()> {
    let mut client = connect_client(&args.base).await?;
    let response = client
        .download_status(DownloadStatusRequest { cid: args.cid })
        .await
        .map_err(|error| anyhow!("DownloadStatus request failed: {error}"))?
        .into_inner();

    println!("cid: {}", response.cid);
    println!("phase: {}", response.phase);
    println!(
        "completed_chunks: {}/{}",
        response.completed_chunks, response.total_chunks
    );
    println!("output_path: {}", response.output_path);
    if !response.error.is_empty() {
        println!("error: {}", response.error);
    }

    Ok(())
}

async fn run_cancel_download(args: ClientCancelDownloadArgs) -> Result<()> {
    let mut client = connect_client(&args.base).await?;
    let response = client
        .cancel_download(CancelDownloadRequest { cid: args.cid })
        .await
        .map_err(|error| anyhow!("CancelDownload request failed: {error}"))?
        .into_inner();

    println!("cancelled: {}", response.cancelled);
    Ok(())
}

async fn run_peers(args: ClientBaseArgs) -> Result<()> {
    let mut client = connect_client(&args).await?;
    let response = client
        .peers(Empty {})
        .await
        .map_err(|error| anyhow!("Peers request failed: {error}"))?
        .into_inner();

    for peer in response.peers {
        println!(
            "peer={} connected={} dialing={} addrs=[{}]",
            peer.peer_id,
            peer.connected,
            peer.dialing,
            peer.addresses.join(", ")
        );
    }

    Ok(())
}

async fn connect_client(
    base: &ClientBaseArgs,
) -> Result<DfsControlClient<tonic::transport::Channel>> {
    let endpoint = normalize_endpoint(&base.grpc_addr);
    DfsControlClient::connect(endpoint)
        .await
        .map_err(|error| anyhow!("failed to connect to local daemon: {error}"))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
