mod bedrock;
use bedrock::BedrockClient;
//use native_tls::TlsConnector;
//use postgres_native_tls::MakeTlsConnector;
//use futures::channel::mpsc;
//use futures::{stream, FutureExt, StreamExt};
use std::str::FromStr;
//use pg_embed::pg_enums::PgAuthMethod;
//use pg_embed::pg_fetch::{PgFetchSettings, PG_V15};
//use pg_embed::postgres::{PgEmbed, PgSettings};
//use std::error::Error;
//use std::path::PathBuf;
//use tokio::fs;
//use tokio::net::TcpListener;
//use tokio::time::Duration;
//use tokio::time::sleep;
//use tokio_postgres::config::Config;
use serde::{Serialize, Deserialize};
//use tokio::select;
use kinode_process_lib::{
    await_message,
    get_typed_state,
    set_state,
//    vfs::create_drive,
    sqlite,
//    vfs::create_file,
    print_to_terminal,
    Address,
    Message,
//    ProcessId,
//    Request,
//    Response,
};

wit_bindgen::generate!({
    path: "wit",
    world: "process",
    exports: {
        world: Component,
    },
});

struct Component;
impl Guest for Component {
    fn init(our: String) {
        print_to_terminal(0, "bedrock: begin");
        let our = Address::from_str(&our).unwrap();
        let sql_client = sqlite::open(our.package_id(), "bedrock").unwrap();
        let mut state: BedrockState = load_bedrock_state(our.clone());
        // 4. start bedrock client to initialze db tables
        let bedrock = BedrockClient::new(sql_client, our);

        /*
        // if the dbstr is emtpy, means we need to initialize postgres
        if state.dbstr == "".to_string() {
            let drive_path: String = create_drive(our.package_id(), ".holon").unwrap();

            let mut port = 5432;
            let mut port_available = false;
            while !port_available {
                // check if port is available
                // if not, increment port by 1 and try again
                let listener = TcpListener::bind(format!("localhost:{}", port)).await;
                match listener {
                    Ok(_) => {
                        port_available = true;
                    }
                    Err(_) => {
                        port += 1;
                    }
                }
            }

            // Postgresql settings
            let pg_settings = PgSettings {
                database_dir: PathBuf::from(drive_path),
                port,
                user: "postgres".to_string(),
                password: "password".to_string(),
                auth_method: PgAuthMethod::Plain,
                persistent: true,
                timeout: Some(Duration::from_secs(15)),
                migration_dir: None,
            };

            // Postgresql binaries download settings
            let fetch_settings = PgFetchSettings {
                version: PG_V15,
                ..Default::default()
            };

            // Create and initialize PgEmbed
            let mut pg = match PgEmbed::new(pg_settings, fetch_settings).await {
                Ok(pg) => pg,
                Err(err) => {
                    print_to_terminal(0, "Failed to init pg embed: {err}");
                    return;
                }
            };

            let _ = match pg.setup().await {
                Ok(_) => (),
                Err(err) => {
                    print_to_terminal(0, "Failed to setup postgres: {err}");
                    return;
                }
            };

            print_to_terminal(0,"postgres setup");

            let _ = match pg.start_db().await {
                Ok(_) => (),
                Err(err) => {
                    print_to_terminal(0, "Failed to start db: {err}");
                    return;
                }
            };

            print_to_terminal(0, "postgres started");

            state.dbstr = format!(
                "postgresql://{}:{}@localhost:{}/bedrock",
                pg.pg_settings.user.to_string(),
                pg.pg_settings.password.to_string(),
                pg.pg_settings.port,
            );
            save_bedrock_state(&state);

            // check if db is already created
            if pg.database_exists("bedrock").await.unwrap() {
                print_to_terminal(0, "bedrock database already exists");
            } else {
                print_to_terminal(0, "creating bedrock database");
                let _ = match pg.create_database("bedrock").await {
                    Ok(_) => (),
                    Err(err) => {
                        print_to_terminal(0, "Failed to create_database: {err}");

                        return;
                    }
                };
            }
        }
    */

        /*
        // subscribe to the postgres notifications
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build().unwrap();
        let connector = MakeTlsConnector::new(connector);
        let (client, mut conn) = tokio_postgres::connect(&state.dbstr, connector)
            .await
            .expect("database connection problem");

        let (tx, mut rx) = mpsc::unbounded();
        let stream = stream::poll_fn(move |cx| conn.poll_message(cx).map_err(|e| panic!("{}", e)));
        let c = stream.forward(tx).map(|r| r.unwrap());
        tokio::spawn(c);
        client
            .batch_execute("LISTEN pending_messages;")
            .await
            .unwrap();
        */

        loop {
            // Call await_message() to wait for any incoming messages.
            // If we get a network error, make a print and throw it away.
            // In a high-quality consumer-grade app, we'd want to explicitly handle
            // this and surface it to the user.
            match await_message() {
                Err(send_error) => {
                    println!("{}: got network error: {send_error:?}", bedrock.our);
                    continue;
                }
                Ok(message) => match handle_message(message, &mut state, &bedrock) {
                    Ok(()) => continue,
                    Err(e) => println!("{}: error handling request: {:?}", bedrock.our, e),
                },
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BedrockState {
    pub our: Address,
    pub dbstr: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum BedrockRequest {
    AddPath {target: String, data: bedrock::SubRequest },
    AddRow {target: String, data: bedrock::SubRequest },
    AddPeer {target: String, data: bedrock::SubRequest },

    UpdPath {target: String, data: bedrock::SubRequest },
    UpdPeer {target: String, data: bedrock::SubRequest },
    UpdRow {target: String, data: bedrock::SubRequest },

    DelPath {target: String, data: bedrock::SubRequest },
    DelPeer {target: String, data: bedrock::SubRequest },
    DelRow {target: String, data: bedrock::SubRequest },

    // wants are for when a non-host peer *wants* the host to take some action on their behalf,
    // thus there's no need for a WantAddPath, because you can't want someone else to add a path
    // for you, you'd just do it yourself.
    WantAddRow {target: String, data: bedrock::SubRequest },
    WantAddPeer {target: String, data: bedrock::SubRequest },

    WantUpdPath {target: String, data: bedrock::SubRequest },
    WantUpdPeer {target: String, data: bedrock::SubRequest },
    WantUpdRow {target: String, data: bedrock::SubRequest },

    WantDelPath {target: String, data: bedrock::SubRequest },
    WantDelPeer {target: String, data: bedrock::SubRequest },
    WantDelRow {target: String, data: bedrock::SubRequest },
}

#[derive(Debug, Serialize, Deserialize)]
enum BedrockResponse {
    Ack { id: i32 },
    Nack
}

fn handle_message (
    system_message: Message,
    state: &mut BedrockState,
    bedrock: &BedrockClient
) -> anyhow::Result<()> {
    let source = system_message.source();
    match system_message {
        Message::Response { ref source, ref body, .. } => {
            if let Ok(response) = serde_json::from_slice::<BedrockResponse>(body) {
                match response {
                    BedrockResponse::Ack { id } => {
                        print_to_terminal(0, "ack from {source}");
                    }
                    BedrockResponse::Nack => {
                        print_to_terminal(0, "nack from {source}");
                    }
                };
            } else {
                print_to_terminal(0, &format!("bedrock: unexpected Response: {:?}", system_message));
                panic!("");
            }
        },

        Message::Request { ref source, ref body, .. } => {
            match serde_json::from_slice(body)? {
                BedrockRequest::AddPath { target, data } => {
                    // we are creating a path
                    if source.node == bedrock.our.node {
                        if let Some(path) = data.path {
                            if let Some(peers) = data.peers {
                                println!("saving new path, since foreign peer {source} added us to his path");
                                let result = bedrock.foreign_new_path(path, source.to_string(), peers);
                                //respond(&mut swarm, channel, request.id, result);
                            }
                        }
                    } else { // some node is trying to add us to a path
                    }
                }
                BedrockRequest::AddPeer { target, data } => {
                    // we are adding a peer
                    if source.node == bedrock.our.node {
                    } else { // the host (presumably) is adding a peer to a path we are in
                    }
                }
                BedrockRequest::AddRow { target, data } => {
                    // we are adding a row
                    if source.node == bedrock.our.node {
                    } else { // the host (presumably) is letting us know about a new row for a path we are in
                    }
                }
                BedrockRequest::UpdPath { target, data } => {
                    // we are updating a path we are in
                    if source.node == bedrock.our.node {
                    } else { // the host (presumably) is letting us know about a path edit
                    }
                }
                BedrockRequest::UpdPeer { target, data } => {
                    // we are updating a peer (changing their role)
                    if source.node == bedrock.our.node {
                    } else { // the host (presumably) is letting us know about a peer edit
                    }
                }
                BedrockRequest::UpdRow { target, data } => {
                    // we are updating a row
                    if source.node == bedrock.our.node {
                    } else { // the host (presumably) is letting us know about a row edit
                    }
                }
                BedrockRequest::DelPath { target, data } => {
                    // we are trying to 
                    if source.node == bedrock.our.node {
                    } else { // the host
                    }
                }
                BedrockRequest::DelPeer { target, data } => {
                    // we are trying to 
                    if source.node == bedrock.our.node {
                    } else { // the host
                    }
                }
                BedrockRequest::DelRow { target, data } => {
                    // we are trying to 
                    if source.node == bedrock.our.node {
                    } else { // the host
                    }
                }

                BedrockRequest::WantAddPeer { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantAddRow { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantUpdPath { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantUpdPeer { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantUpdRow { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantDelPath { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantDelPeer { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
                BedrockRequest::WantDelRow { target, data } => {
                    if source.node == bedrock.our.node {
                        // error
                    } else { // we must be the host here, or else this is an error
                    }
                }
            }
        },
    }
    Ok(())
}


/// Helper function to deserialize the process state. Note that we use a helper function
/// from process_lib to fetch a typed state, which will return None if the state does
/// not exist OR fails to deserialize. In either case, we'll make an empty new state.
fn load_bedrock_state(our: Address) -> BedrockState {
    match get_typed_state(|bytes| Ok(bincode::deserialize::<BedrockState>(bytes)?)) {
        Some(b) => b,
        None => BedrockState {
            our,
            dbstr: "".to_string(),
        },
    }
}

/// Helper function to serialize and save the process state.
fn save_bedrock_state(state: &BedrockState) {
    set_state(&bincode::serialize(&state).unwrap());
}

