use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
//use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
//use std::fmt::Write;
//use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;
use kinode_process_lib::{
    print_to_terminal,
    Address,
    sqlite::Sqlite,
};

const DEFAULT_TABLES: [&str; 8] = [
    "all_peers",
    "self_info",
    "peers",
    "paths",
    "access_rules",
    "constraints",
    // TODO store schemas and make a way for devs to query them
    "schemas",
    "pending_messages",
];

pub struct BedrockClient {
    //pub client: tokio_postgres::Client,
    pub client: Sqlite,
    pub our: Address,
}

impl BedrockClient {
    pub fn new(client: Sqlite, our: Address) -> BedrockClient {
        // 1. connect to and setup postgres tables (if they aren't already setup)
        // 2. generate/read-from-db a keypair + PeerId
        // let peerId = PeerId::random();
        // let key = identity::Keypair::generate_secp256k1();

        // 2b. create /{id} and /{id}/private paths if they aren't already there
        // 3. setup passport

        // 1. connect to and setup postgres tables (if they aren't already setup)
        // bedrock formatting note:
        // set `type` to 'default' to define custom default access_rules for a path+role combo, othwerwise must match table name of row type
        let creates = [
            "CREATE TABLE IF NOT EXISTS self_info (
                id      TEXT PRIMARY KEY,
                id_bytes    BLOB NOT NULL,
                key     BLOB NOT NULL,
                multiaddr  TEXT
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS all_peers (
                name    TEXT PRIMARY KEY,
                id      TEXT NOT NULL,
                addr    TEXT
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS peers (
                path    TEXT NOT NULL,
                id      TEXT NOT NULL,
                role    TEXT NOT NULL DEFAULT 'member',
                metadata     TEXT,
                created_at   INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL,
                received_at  INTEGER NOT NULL,
                PRIMARY KEY (path, id)
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS paths (
                path    TEXT PRIMARY KEY,
                host    TEXT NOT NULL,
                metadata     TEXT,
                replication  TEXT NOT NULL DEFAULT 'Host',
                security     TEXT NOT NULL DEFAULT 'host-invite',
                created_at   INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL,
                received_at  INTEGER NOT NULL
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS access_rules (
                path    TEXT NOT NULL,
                type    TEXT NOT NULL,
                role    TEXT NOT NULL,
                creat   BOOLEAN DEFAULT true,
                edit    TEXT DEFAULT 'own',
                delet   TEXT DEFAULT 'own',
                PRIMARY KEY (path, type, role)
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS constraints (
                path    TEXT NOT NULL,
                type    TEXT NOT NULL,
                uniques TEXT,
                checks  TEXT,
                PRIMARY KEY (path, type)
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS schemas (
                type    TEXT PRIMARY KEY,
                schema  TEXT NOT NULL
            );".to_string(),
            "CREATE TABLE IF NOT EXISTS pending_messages (
                id      SERIAL PRIMARY KEY,
                type    TEXT NOT NULL,
                target  TEXT NOT NULL,
                msg     TEXT NOT NULL
            );".to_string(),
        ];
        for c in creates {
            let r = client.write(c, vec![], None);
            print_to_terminal(0, &format!("{:?}",r));
        }

        // 2. create /{id} and /{id}/private paths if they aren't already there
        create_path(
            &client,
            &our,
            Path {
                path: format!("/{our}"),
                host: our.to_string(),
                metadata: json!({}),
                replication: PathReplication::Host,
                access_rules: HashMap::new(),
                security: PathSecurity::Public,
                created_at: sys_time_to_u64(SystemTime::now()),
                updated_at: sys_time_to_u64(SystemTime::now()),
                received_at: sys_time_to_u64(SystemTime::now()),
            },
        );
        create_path(
            &client,
            &our,
            Path {
                path: format!("/{our}/private"),
                host: our.to_string(),
                metadata: json!({}),
                replication: PathReplication::Host,
                access_rules: HashMap::new(),
                security: PathSecurity::HostInvite,
                created_at: sys_time_to_u64(SystemTime::now()),
                updated_at: sys_time_to_u64(SystemTime::now()),
                received_at: sys_time_to_u64(SystemTime::now()),
            },
        );

        BedrockClient {
            client,
            our: our.clone(),
        }
    }

    /*
    pub fn cleanup(&self) {
        let drops = [
            "DROP TABLE IF EXISTS self_info;".to_string(),
            "DROP TABLE IF EXISTS all_peers;".to_string(),
            "DROP TABLE IF EXISTS peers;".to_string(),
            "DROP TABLE IF EXISTS paths;".to_string(),
            "DROP TABLE IF EXISTS access_rules;".to_string(),
            "DROP TABLE IF EXISTS constraints;".to_string(),
            "DROP TABLE IF EXISTS schemas;".to_string(),
            "DROP TABLE IF EXISTS pending_messages;".to_string()
        ];
        for d in drops {
            let _ = self.client.write(d, vec![], None);
        }

        let tbl_names = self
            .client
            .read(
                "select table_name from information_schema.tables where table_schema = 'public';".to_string(),
                vec![],
            )
            .unwrap();
        for name_row in tbl_names {
            let name: String = name_row.get("table_name").unwrap().as_str().unwrap().to_string();
            let _ = self
                .client
                .write(format!("DROP TABLE IF EXISTS {}", name), vec![], None);
        }
    }

    */
    pub fn create_path(&self, path: Path) -> bool {
        create_path(&self.client, &self.our, path)
    }

    pub fn update_path(&self, path: Path) -> Result<(),()> {
        let str_path = path.path.clone();
        if !we_are_in_path(&str_path, &self.client) { return Err(()); }

        let host = self.host_for(&path.path).unwrap();
        //if we are the path host, save the row and tell all the peers
        if host == self.our.to_string() {
            print_to_terminal(0, "we are host, update_path");
            // TODO: detect host change and update peer table accordingly
            let _ = self.client.write(
               "UPDATE paths SET (metadata, host, replication, security, updated_at, received_at) = ($1, $2, $3, $4, $5, $6) WHERE path = $7".to_string(),
                vec![path.metadata.clone(), Value::String(path.host.clone()), Value::String(path.replication.to_string()), Value::String(path.security.to_string()), serde_json::to_value(path.updated_at).unwrap(), serde_json::to_value(path.received_at).unwrap(), Value::String(str_path)],
                None,
            );
            // create the pending_message s
            let peers = self.get_peers(&path.path).unwrap();
            let req = SubRequest {path: Some(path), peers: None, rows: None, ids: None};
            // tell the peers about the updated peer
            for p in peers {
                if p.id == self.our.to_string() { continue; } // don't need to tell ourself

                let _ = self.client.write(
                    "INSERT INTO pending_messages (type, target, msg) VALUES ('upd-path', $1, $2)".to_string(),
                    vec![Value::String(p.id), serde_json::to_value(&req).unwrap()],
                    None,
                );
            }
        } else {
            let req = SubRequest {path: Some(path), peers: None, rows: None, ids: None};
            // we are not the host, so send the path to the host to update
            let _ = self.client.write(
                String::from("INSERT INTO pending_messages (type, target, msg) VALUES ('want-upd-path', $1, $2)"),
                vec![Value::String(host), serde_json::to_value(&req).unwrap()],
                None
            );
        }

        Ok(())
    }

    pub fn delete_path(&self, path: &str) -> Result<(),()> {
        // when we want to delete a path, it's the same logic as if we 
        // are removing ourselves from the path
        self.delete_peer(self.our.to_string().as_str(), path)
    }

    pub fn add_peer(&self, path: &str, peer: Peer) {
        // first, sanity check that the peer isn't already in the peerlist
        let peers = peers_for(path, &self.client);
        if peers.contains(&peer.id) {
            print_to_terminal(0, "peer is already in the path");
            return;
        }

        let _peerreq = SubRequest {path: None, peers: Some(vec![peer.clone()]), rows: None, ids: None};
        let host = self.host_for(path).unwrap();
        //if we are the path host, save the row and tell all the peers
        if host == self.our.to_string() {
            print_to_terminal(0, "we are the host... saving new peer");
            let _ = self.client.write(
                String::from("INSERT INTO peers (path, id, role, metadata, created_at, updated_at, received_at) VALUES ($1, $2, $3, $4, $5, $6, $7)"),
                peer.to_value_vec(),
                None
            );

            // create the pending_message
            let pathobj = self.get_path(path).unwrap();
            let raw_peers = self.get_peers(path).unwrap();
            let peers = Some(raw_peers.clone());
            let subreq = SubRequest {path: Some(pathobj), peers, rows: None, ids: None};
            // tell the new peer about the path
            let _ = self.client.write(
                String::from("INSERT INTO pending_messages (type, target, msg) VALUES ('add-path', $1, $2)"),
                vec![Value::String(peer.id.clone()), serde_json::to_value(&subreq).unwrap()],
                None
            );
            // tell all the peers about the new peer
            for p in raw_peers {
                if p.id == peer.id { continue; } // don't need to tell the peer about adding himself... duh
                if p.id == self.our.to_string() { continue; } // don't need to tell ourself either

                //let _ = self.client.write(
                //    "INSERT INTO pending_messages (type, target, msg) VALUES ('add-peer', $1, $2)",
                //    &[&p.id, &serde_json::to_value(&peerreq).unwrap()]
                //).await;
            }
        } else {
            // we are not the host, so send the peer to the host to add
            //let _ = self.client.execute(
            //    "INSERT INTO pending_messages (type, target, msg) VALUES ('want-add-peer', $1, $2)",
            //    &[&host, &serde_json::to_value(&peerreq).unwrap()]
            //).await;
        }
    }

    pub fn update_peer(&self, peer: Peer) {
        // first, sanity check that the peer is already in the peerlist
        let peers = peers_for(&peer.path, &self.client);
        if !peers.contains(&peer.id) {
            print_to_terminal(0, "peer not in the path");
            return;
        }

        let _peerreq = SubRequest {path: None, peers: Some(vec![peer.clone()]), rows: None, ids: None};
        let host = self.host_for(&peer.path).unwrap();
        //if we are the path host, save the row and tell all the peers
        if host == self.our.to_string() {
            print_to_terminal(0, "we are the host... saving new peer");
            let _ = self.client.write(
               String::from("UPDATE peers SET (role, metadata, updated_at, received_at) = ($1, $2, $3, $4) WHERE path = $5 AND id = $6"),
                vec![Value::String(peer.role), peer.metadata, sys_time_to_value(peer.updated_at), sys_time_to_value(peer.received_at), Value::String(peer.path.clone()), Value::String(peer.id)],
                None
            );

            // create the pending_message s
            let peers = self.get_peers(&peer.path).unwrap();
            // tell the peers about the updated peer
            for p in peers {
                if p.id == self.our.to_string() { continue; } // don't need to tell ourself

                //let _ = self.client.execute(
                //    "INSERT INTO pending_messages (type, target, msg) VALUES ('upd-peer', $1, $2)",
                //    &[&p.id, &serde_json::to_value(&peerreq).unwrap()]
                //);
            }
        } else {
            // we are not the host, so send the peer to the host to add
            //let _ = self.client.execute(
            //    "INSERT INTO pending_messages (type, target, msg) VALUES ('want-upd-peer', $1, $2)",
            //    &[&host, &serde_json::to_value(&peerreq).unwrap()]
            //);
        }
    }

    pub fn delete_peer(&self, id: &str, path: &str) -> Result<(),()> {
        // TODO: permission checking
        if !we_are_in_path(path, &self.client) { return Err(()); }

        // find the peer we want to delete
        //let pathrow = self.get_path(path);
        let peers = self.get_peers(path).unwrap();
        let mut peer: Option<Peer> = None;
        for p in peers.clone() {
            if p.id == id.to_string() {
                peer = Some(p);
                break;
            }
        }
        let peer = peer.unwrap();
        let _subreq = SubRequest {path: None, peers: Some(vec![peer]), rows: None, ids: None};

        let host = self.host_for(&path).unwrap();
        //if we are the path host, delete the peer and tell all the peers
        if host == self.our.to_string() {
            // if we are deleting ourself it's equivalent to del-path, since we're the host
            if self.our.to_string() == id.to_string() {
                self.leave_path_sql(path);
                for p in peers {
                    if p.id == self.our.to_string() { continue; } // don't need to tell ourself
                    print_to_terminal(0, &format!("sending del-peer of themselves (equivalent to del-path) to peer {}", p.id));
                    let subreq = SubRequest {path: None, peers: Some(vec![p.clone()]), rows: None, ids: None};
                    let _ = self.client.write(
                        String::from("INSERT INTO pending_messages (type, target, msg) VALUES ('del-peer', $1, $2)"),
                        vec![Value::String(p.id), serde_json::to_value(&subreq).unwrap()],
                        None
                    );
                }
            } else {
                // delete it locally
                let _ = self.client.write(
                    String::from("DELETE FROM peers WHERE id = $1 AND path = $2"),
                    vec![Value::String(id.to_string()), Value::String(path.to_string())],
                    None
                );
                // tell all the peers that we deleted the peer
                for p in peers {
                    if p.id == self.our.to_string() { continue; } // don't need to tell ourself
                    print_to_terminal(0, &format!("sending del-peer to peer {}", p.id));
                    //let _ = self.client.execute(
                    //    "INSERT INTO pending_messages (type, target, msg) VALUES ('del-peer', $1, $2)",
                    //    &[&p.id, &serde_json::to_value(&subreq).unwrap()]
                    //);
                }
            }
        } else {
            // we are not the host, so send the peer to the host to remove
            //let _ = self.client.execute(
            //    "INSERT INTO pending_messages (type, target, msg) VALUES ('want-del-peer', $1, $2)",
            //    &[&host, &serde_json::to_value(&subreq).unwrap()]
            //);
        }

        Ok(())
    }

    pub fn add_row(&self, row: Row) {
        let pathhost: String = self.host_for(&row.path).unwrap();
        let _subreq = SubRequest {path: None, peers: None, rows: Some(vec![row.clone()]), ids: None};
        //if we are the path host, save the row and tell all the peers
        if pathhost == self.our.to_string() {
            for peer in peers_for(&row.path, &self.client) {
                if peer == self.our.to_string() {
                    continue;
                }

                //let _ = self.client.execute(
                //    "INSERT INTO pending_messages (type, target, msg) VALUES ('add-row', $1, $2)",
                //    &[&peer, &serde_json::to_value(&subreq).unwrap()]
                //).await;
            }
            self.save_row(row, false);
        } else {
            // we are not the host, so send the row to the host to create
            //let _ = self.client.execute(
            //    "INSERT INTO pending_messages (type, target, msg) VALUES ('want-add-row', $1, $2)",
            //    &[&pathhost, &serde_json::to_value(&subreq).unwrap()]
            //).await;
        }
    }

    pub fn update_row(&self, row: Row) {
        let pathhost: String = self.host_for(&row.path).unwrap();
        let subreq = SubRequest {path: None, peers: None, rows: Some(vec![row.clone()]), ids: None};
        //if we are the path host, save the row and tell all the peers
        if pathhost == self.our.to_string() {
            for peer in peers_for(&row.path, &self.client) {
                if peer == self.our.to_string() {
                    continue;
                }

                let _ = self.client.write(
                    String::from("INSERT INTO pending_messages (type, target, msg) VALUES ('upd-row', $1, $2)"),
                    vec![Value::String(peer), serde_json::to_value(&subreq).unwrap()],
                    None
                );
            }
            self.save_row(row, true);
        } else {
            // we are not the host, so send the row to the host to create
            //let _ = self.client.execute(
            //    "INSERT INTO pending_messages (type, target, msg) VALUES ('want-upd-row', $1, $2)",
            //    &[&pathhost, &serde_json::to_value(&subreq).unwrap()]
            //).await;
        }
    }

    pub fn delete_row(&self, tbl: String, id: String, path: String) -> Result<(),()> {
        // TODO: permission checking
        // ensure that the `tbl` is present
        if let Err(_) = table_name_helper(&self.client, tbl.clone()) {
            return Err(());
        }

        let trip = RowIdTriple {
            tbl_type: tbl.clone(),
            id: id.clone(),
            path: path.clone()
        };
        let _subreq = SubRequest {path: None, peers: None, rows: None, ids: Some(vec![trip])};

        let host = self.host_for(&path).unwrap();
        //if we are the path host, delete the row and tell all the peers
        if host == self.our.to_string() {
            // get the row
            // delete it locally
            let _ = self.client.write(
                format!("DELETE FROM {} WHERE id = $1", tbl),
                vec![Value::String(id)],
                None
            );

            // tell all the peers that we deleted the row
            let peers = self.get_peers(&path).unwrap();
            for p in peers {
                if p.id == self.our.to_string() { continue; } // don't need to tell ourself
                print_to_terminal(0, &format!("sending del-row to peer {}", p.id));
                //let _ = self.client.execute(
                //    "INSERT INTO pending_messages (type, target, msg) VALUES ('del-row', $1, $2)",
                //    &[&p.id, &serde_json::to_value(&subreq).unwrap()]
                //).await;
            }
        } else {
            // we are not the host, so send the peer to the host to add
            //let _ = self.client.execute(
            //    "INSERT INTO pending_messages (type, target, msg) VALUES ('want-del-row', $1, $2)",
            //    &[&host, &serde_json::to_value(&subreq).unwrap()]
            //);
        }

        Ok(())
    }

    pub fn foreign_new_row(&self, mut row: Row, peerid: String) -> Result<(),()> {
        // TODO: proper access control checking. does this peer have the right to tell me about
        // this new row, and does this new row have the right to be created on this path?
        //let path = self.get_path(&row.path);

        // we are not in the path, something is wrong
        if !we_are_in_path(&row.path, &self.client) { return Err(()); }

        let peers = peers_for(&row.path, &self.client);
        if !peers.contains(&peerid) { return Err(()); } // only peers can add rows

        row.received_at = SystemTime::now();
        self.save_row(row, false);
        Ok(())
    }

    pub fn foreign_new_path(&self, mut path: Path, peerid: String, peers: Vec<Peer>) -> Result<(),()> {
        if peerid != path.host { return Err(()); } // only host can add us

        let raw = self.client
            .read(String::from("SELECT host FROM paths WHERE path = $1"), vec![Value::String(path.path.clone())])
            .unwrap();
        if raw.len() > 0 { return Err(()); } // we are already in the path

        path.received_at = sys_time_to_u64(SystemTime::now());
        let _ = self.client.write(
           "INSERT INTO paths (path, metadata, host, replication, security, created_at, updated_at, received_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)".to_string(),
            path.to_value_vec(),
            None
        );

        // insert the peers
        for peer in peers {
            let _ = self.client.write(
                "INSERT INTO peers (path, id, role, metadata, created_at, updated_at, received_at) VALUES ($1, $2, $3, $4, $5, $6, $7)".to_string(),
                peer.to_value_vec(),
                None
            );
        }

        Ok(())
    }

    pub fn foreign_new_peer(&self, mut peer: Peer, foreign_peerid: String) -> Result<(),()> {
        let path = self.get_path(&peer.path);
        if let Err(_) = path { return Err(()); }
        let path = path.unwrap();
        if foreign_peerid != path.host { return Err(()); } // only host can add peers

        let raw = self.client
            .read("SELECT host FROM paths WHERE path = $1".to_string(), vec![Value::String(path.path)])
            .unwrap();
        if raw.len() == 0 { return Err(()); } // we aren't in the path

        peer.received_at = SystemTime::now();
        let _ = self.client.write(
            String::from("INSERT INTO peers (path, id, role, metadata, created_at, updated_at, received_at) VALUES ($1, $2, $3, $4, $5, $6, $7)"),
            peer.to_value_vec(),
            None
        );
        Ok(())
    }

    pub fn foreign_upd_peer(&self, mut peer: Peer, foreign_peerid: String) -> Result<(),()> {
        if !we_are_in_path(&peer.path, &self.client) { return Err(()); }

        let path = self.get_path(&peer.path);
        if let Err(_) = path { return Err(()); }

        if foreign_peerid != path.unwrap().host { return Err(()); } // only host can tell us to edit peer

        peer.received_at = SystemTime::now();
        let _ = self.client.write(
           "UPDATE peers SET (role, metadata, updated_at, received_at) = ($1, $2, $3, $4) WHERE id = $5 and path = $6".to_string(),
            vec![Value::String(peer.role), peer.metadata, sys_time_to_value(peer.updated_at), sys_time_to_value(peer.received_at), Value::String(peer.id), Value::String(peer.path)],
            None
        );

        Ok(())
    }

    pub fn foreign_upd_row(&self, mut row: Row, peerid: String) -> Result<(),()> {
        // TODO: proper access control checking. does this peer have the right to tell me about
        // this row edit, and is this row editable on this path?
        //let path = self.get_path(&row.path);

        // we are not in the path, something is wrong
        if !we_are_in_path(&row.path, &self.client) { return Err(()); }

        let peers = peers_for(&row.path, &self.client);
        if !peers.contains(&peerid) { return Err(()); } // only peers can update rows

        row.received_at = SystemTime::now();
        self.save_row(row, true);
        Ok(())
    }

    pub fn foreign_upd_path(&self, mut path: Path, peerid: String) -> Result<(),()> {
        if !we_are_in_path(&path.path, &self.client) { return Err(()); }

        let peers = peers_for(&path.path, &self.client);
        if !peers.contains(&peerid) { return Err(()); } // only peers can edit the path

        let peer_objs = self.get_peers(&path.path).unwrap();
        let req_role = peer_objs.iter().find(|&pr| pr.id == peerid).unwrap().role.clone();
        if !["host", "admin"].contains(&req_role.as_str()) { return Err(()); } // only host and admins can update path

        // update it
        path.received_at = sys_time_to_u64(SystemTime::now());
        // TODO: detect host change and update peer table accordingly
        let _ = self.client.write(
           String::from("UPDATE paths SET (metadata, host, replication, security, updated_at, received_at) = ($1, $2, $3, $4, $5, $6) WHERE path = $7"),
            vec![
                path.metadata,
                Value::String(path.host),
                Value::String(path.replication.to_string()),
                Value::String(path.security.to_string()),
                serde_json::to_value(path.updated_at).unwrap(),
                serde_json::to_value(path.received_at).unwrap(),
                Value::String(path.path)
            ],
            None
        );

        Ok(())
    }

    pub fn foreign_del_peer(&self, peer: Peer, foreign_peerid: String) -> Result<(),()> {
        // TODO: proper access control checking. does this peer have the right to tell me about
        // this peer removal?
        if !we_are_in_path(&peer.path, &self.client) { return Err(()); } // we are not in the path, something is wrong

        let path = self.get_path(&peer.path).unwrap();
        if path.host != foreign_peerid { return Err(()); } // for now, only host can remove peers

        let peers = peers_for(&peer.path, &self.client);
        if !peers.contains(&peer.id) { return Err(()); } // only remove peers who are actually present

        // if we are the one being deleted... gotta do a little more logic
        if peer.id == self.our.to_string() {
            self.leave_path_sql(&path.path);
        } else {
            let _ = self.client.write(
                "DELETE FROM peers WHERE id = $1 AND path = $2".to_string(),
                vec![Value::String(peer.id), Value::String(peer.path)],
                None
            );
        }
        Ok(())
    }

    pub fn foreign_del_row(&self, id: RowIdTriple, foreign_peerid: String) -> Result<(),()> {
        // TODO: proper access control checking. does this peer have the right to tell me about
        // this deletion, and does this row have the right to be deleted?
        //let path = self.get_path(&row.path);
        let raw = self.client
            .read(String::from("SELECT host FROM paths WHERE path = $1"), vec![Value::String(id.path.clone())])
            .unwrap();
        if raw.len() == 0 { return Err(()); } // we are not in the path, something is wrong

        let peers = peers_for(&id.path, &self.client);
        if !peers.contains(&foreign_peerid) { return Err(()); } // only peers can add rows

        // ensure that the table is present
        if let Err(_) = table_name_helper(&self.client, id.tbl_type.clone()) {
            return Err(());
        }

        let _ = self.client.write(
            format!("DELETE FROM {} WHERE id = $1", id.tbl_type),
            vec![Value::String(id.id)],
            None
        );
        Ok(())
    }

    /*
    pub fn pending_messages(&self) -> Vec<BedrockRequest> {
        let raw = self
            .client
            .read("SELECT * FROM pending_messages".to_string(), [])
            .unwrap();

        let mut results: Vec<BedrockRequest> = Vec::with_capacity(raw.len());
        for r in raw {
            let id: i32 = r.get("id");
            let v: Value = r.get("msg");
            let data: SubRequest = serde_json::from_value(v).unwrap();
            let t: String = r.get("type");
            let kind = t.parse::<BedrockMessageType>().unwrap();
            let target: String = r.get("target");
            results.push(BedrockRequest { id, kind, target, data});
        }

        results
    }

    pub fn clear_pending_message(&self, id: i32) {
        let _ = self.client.execute(
           "DELETE FROM pending_messages WHERE id = $1",
            &[&id],
        );
    }

    */
    fn host_for(&self, path: &str) -> Result<String, ()> {
        let raw = self
            .client
            .read(
                String::from("SELECT host FROM paths WHERE path = $1"),
                vec![Value::String(String::from(path))],
            )
            .unwrap();
        if raw.len() == 0 {
            Err(())
        } else {
            let result = raw[0].get("host");
            match result {
                Some(r) => Ok(r.as_str().unwrap().to_string()),
                None => Err(())
            }
        }
    }

    fn save_row(&self, row: Row, update: bool) {
        let tbl_name = row.tbl_name();
        let unique_row_id = row.id_string();
        let mut row_values: Vec<Value> = vec![
            Value::String(row.path),
            Value::String(unique_row_id),
            Value::String(row.id.0),
            sys_time_to_value(row.created_at),
            sys_time_to_value(row.updated_at),
            sys_time_to_value(row.received_at),
        ];
        let mut cols = String::new();
        let mut vals = String::new();
        let mut i = 7; // 1-6 are the metadata columns every table has
        let mut reals: [f64; 128] = [0.0; 128];
        let mut realc = 0;
        let mut ints: [i64; 128] = [0; 128];
        let mut intc = 0;
        if let Some(schema) = row.schema.clone() {
            for c in &schema {
                if let Some(val) = row.data.get(&c.0) {
                    match val {
                        Value::Number(n) => {
                            if n.is_f64() {
                                reals[realc] = n.as_f64().unwrap();
                                realc += 1;
                            }
                            if n.is_i64() {
                                ints[intc] = n.as_i64().unwrap();
                                intc += 1;
                            }
                            ()
                        }
                        _ => (),
                    }
                }
            }
        }
        if let Some(schema) = row.schema.clone() {
            // 1. generate table-definition from row.schema
            let mut tbl_def: String = String::from("");
            for c in &schema {
                let col = format!("{}   {}", c.0, c.1);
                if i == 7 {
                    cols = format!("{}", c.0);
                    vals = format!("${}", i);
                    tbl_def = format!("{}", col);
                } else {
                    cols = format!("{}, {}", cols, c.0);
                    vals = format!("{}, ${}", vals, i);
                    tbl_def = format!("{},\n{}", tbl_def, col);
                }

                if let Some(val) = row.data.get(&c.0) {
                    row_values.push(val.clone());
                }

                i += 1;
            }
            // 2. create the table for the row IF NOT EXISTS
            let stmt = format!(
                "CREATE TABLE IF NOT EXISTS {} (
                        path   text not null,
                        id     text primary key,
                        sender  text not null,
                        created_at   INTEGER NOT NULL,
                        updated_at   INTEGER NOT NULL,
                        received_at  INTEGER NOT NULL,
                      {})",
                tbl_name, tbl_def
            );
            let _ = self.client.write(stmt, vec![], None);
        }
        // 3. INSERT the row
        let stmt = if update {
            format!("UPDATE {} SET (path, id, sender, created_at, updated_at, received_at, {}) = ($1, $2, $3, $4, $5, $6, {}) WHERE id = $2", tbl_name, cols, vals)
        } else {
            format!("INSERT INTO {} (path, id, sender, created_at, updated_at, received_at, {}) VALUES ($1, $2, $3, $4, $5, $6, {})", tbl_name, cols, vals)
        };
        print_to_terminal(0, &format!("save row: {}, {:?}",stmt, row_values));

        let _ = self.client.write(stmt, row_values, None);
    }

    pub fn get_path(&self, path: &str) -> Result<Path, ()> {
        get_path(&self.client, path)
    }

    pub fn get_peers(&self, path: &str) -> Result<Vec<Peer>, ()> {
        let raw = self
            .client
            .read("SELECT * FROM peers WHERE path = $1".to_string(), vec![Value::String(path.to_string())]);

        match raw {
            Err(_) => Err(()),
            Ok(raw) => {
                if raw.len() == 0 {
                    Err(())
                } else {
                    let mut peers = Vec::with_capacity(raw.len());
                    for row in raw {
                        peers.push(Peer {
                            path: row.get("path").unwrap().as_str().unwrap().to_string(),
                            id: row.get("id").unwrap().as_str().unwrap().to_string(),
                            role: row.get("role").unwrap().as_str().unwrap().to_string(),
                            metadata: row.get("metadata").unwrap().clone(),
                            created_at: value_to_sys(row.get("created_at").unwrap()),
                            updated_at: value_to_sys(row.get("updated_at").unwrap()),
                            received_at: value_to_sys(row.get("received_at").unwrap()),
                        });
                    }
                    Ok(peers)
                }
            }
        }
    }

    /*
    pub async fn save_peer_connection_info(&self, name: Option<String>, id: String, addr: Option<String>) {
        match name {
            None => {
                if let Some(address) = addr {
                    let res = self.client.query(
                        "SELECT * FROM all_peers WHERE id = $1",
                        &[&id],
                    ).await;
                    if res.unwrap().len() == 0 {
                        let peers = self.passport.get_peers().await.unwrap();
                        let mut name = String::new();
                        for peer in peers {
                            if peer.peer_id == id {
                                name = peer.name;
                            }
                        }
                        if name.is_empty() {
                            eprintln!("could not save peer id: {}, because no matching name is found on chain", id);
                        } else {
                            let _ = self.client.execute(
                                "INSERT INTO all_peers (name, id, addr) VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE SET addr = EXCLUDED.addr",
                                &[&name, &id, &address],
                            ).await;
                        }
                    } else {
                        let _ = self.client.execute(
                            "UPDATE all_peers SET addr = $1 WHERE id = $2",
                            &[&address, &id],
                        ).await;
                    }
                } else {
                    eprintln!("could not save peer id: {}, because both addr and name are None", id);
                }
            },
            Some(n) => {
                match addr {
                    Some(address) => {
                        let _ = self.client.execute(
                            "INSERT INTO all_peers (name, id, addr) VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE SET addr = EXCLUDED.addr",
                            &[&n, &id, &address],
                        ).await;
                    },
                    None => {
                        let _ = self.client.execute(
                            "INSERT INTO all_peers (name, id) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET id = EXCLUDED.id",
                            &[&n, &id],
                        ).await;
                    }
                }
            }
        }
    }
    */
    fn leave_path_sql(&self, path: &str) {
        // delete all the rows of actual data that were on the path
        let tbl_names = self.client.read(
            "select table_name from information_schema.tables where table_schema = 'public';".to_string(),
            vec![],
        ).unwrap();
        for name_row in tbl_names {
            let name = name_row.get("table_name").unwrap().as_str().unwrap();
            if DEFAULT_TABLES.contains(&name) {
                continue;
            }
            let _ = self.client.write(
                format!("DELETE FROM {} WHERE path = $1", name),
                vec![Value::String(path.to_string())],
                None
            );
        }

        // delete the path
        let _ = self.client.write(
            "DELETE FROM paths WHERE path = $1".to_string(),
            vec![Value::String(path.to_string())],
            None
        );

        // delete all the peers on that path
        let _ = self.client.write(
            "DELETE FROM peers WHERE path = $1".to_string(),
            vec![Value::String(path.to_string())],
            None
        );

    }

}

#[derive(Debug, Serialize, Deserialize)]
pub enum BedrockMessageType {
    AddPath,
    AddRow,
    AddPeer,

    UpdPath,
    UpdPeer,
    UpdRow,

    DelPath,
    DelPeer,
    DelRow,

    WantAddRow,
    WantAddPeer,

    WantUpdPath,
    WantUpdPeer,
    WantUpdRow,

    WantDelPath,
    WantDelPeer,
    WantDelRow,
}
// gives us the .to_string()
impl std::fmt::Display for BedrockMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BedrockMessageType::AddPath => write!(f, "add-path"),
            BedrockMessageType::AddRow => write!(f, "add-row"),
            BedrockMessageType::AddPeer => write!(f, "add-peer"),

            BedrockMessageType::UpdPath => write!(f, "upd-path"),
            BedrockMessageType::UpdRow => write!(f, "upd-row"),
            BedrockMessageType::UpdPeer => write!(f, "upd-peer"),

            BedrockMessageType::DelPath => write!(f, "del-path"),
            BedrockMessageType::DelRow => write!(f, "del-row"),
            BedrockMessageType::DelPeer => write!(f, "del-peer"),

            BedrockMessageType::WantAddRow => write!(f, "want-add-row"),
            BedrockMessageType::WantAddPeer => write!(f, "want-add-peer"),

            BedrockMessageType::WantUpdPath => write!(f, "want-upd-path"),
            BedrockMessageType::WantUpdRow => write!(f, "want-upd-row"),
            BedrockMessageType::WantUpdPeer => write!(f, "want-upd-peer"),

            BedrockMessageType::WantDelPath => write!(f, "want-del-path"),
            BedrockMessageType::WantDelRow => write!(f, "want-del-row"),
            BedrockMessageType::WantDelPeer => write!(f, "want-del-peer"),
        }
    }
}
impl FromStr for BedrockMessageType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "add-path" => Ok(BedrockMessageType::AddPath),
            "add-row" => Ok(BedrockMessageType::AddRow),
            "add-peer" => Ok(BedrockMessageType::AddPeer),

            "upd-path" => Ok(BedrockMessageType::UpdPath),
            "upd-row" => Ok(BedrockMessageType::UpdRow),
            "upd-peer" => Ok(BedrockMessageType::UpdPeer),

            "del-path" => Ok(BedrockMessageType::DelPath),
            "del-row" => Ok(BedrockMessageType::DelRow),
            "del-peer" => Ok(BedrockMessageType::DelPeer),

            "want-add-row" => Ok(BedrockMessageType::WantAddRow),
            "want-add-peer" => Ok(BedrockMessageType::WantAddPeer),

            "want-upd-path" => Ok(BedrockMessageType::WantUpdPath),
            "want-upd-row" => Ok(BedrockMessageType::WantUpdRow),
            "want-upd-peer" => Ok(BedrockMessageType::WantUpdPeer),

            "want-del-path" => Ok(BedrockMessageType::WantDelPath),
            "want-del-row" => Ok(BedrockMessageType::WantDelRow),
            "want-del-peer" => Ok(BedrockMessageType::WantDelPeer),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub path: String,
    pub id: String,
    pub role: String,
    pub metadata: Value,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub received_at: SystemTime,
}
impl Peer {
    fn to_value_vec(&self) -> Vec<Value> {
       // always in the following order:
       // path, id, role, metadata, created_at, updated_at, received_at
        vec![
            Value::String(self.path.clone()),
            Value::String(self.id.clone()),
            Value::String(self.role.clone()),
            Value::String(serde_json::to_string(&self.metadata).unwrap()),
            sys_time_to_value(self.created_at),
            sys_time_to_value(self.updated_at),
            sys_time_to_value(self.received_at),
        ]
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PathReplication {
    Host,
    MultiHost,
    Gossip,
}
// gives us the .to_string()
impl std::fmt::Display for PathReplication {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PathReplication::Host => write!(f, "Host"),
            PathReplication::MultiHost => write!(f, "MultiHost"),
            PathReplication::Gossip => write!(f, "Gossip"),
        }
    }
}
impl FromStr for PathReplication {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Host" => Ok(PathReplication::Host),
            "MultiHost" => Ok(PathReplication::MultiHost),
            "Gossip" => Ok(PathReplication::Gossip),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum PathSecurity {
    Public,
    MemberInvite,
    HostInvite,
    NftGated,
}
// gives us the .to_string()
impl std::fmt::Display for PathSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PathSecurity::Public => write!(f, "Public"),
            PathSecurity::MemberInvite => write!(f, "MemberInvite"),
            PathSecurity::HostInvite => write!(f, "HostInvite"),
            PathSecurity::NftGated => write!(f, "NftGated"),
        }
    }
}
impl FromStr for PathSecurity {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "public" => Ok(PathSecurity::Public),
            "open" => Ok(PathSecurity::Public),
            "member-invite" => Ok(PathSecurity::MemberInvite),
            "anyone" => Ok(PathSecurity::MemberInvite),
            "host-invite" => Ok(PathSecurity::HostInvite),
            "nft-gated" => Ok(PathSecurity::NftGated),
            "host" => Ok(PathSecurity::HostInvite),

            "Public" => Ok(PathSecurity::Public),
            "MemberInvite" => Ok(PathSecurity::MemberInvite),
            "HostInvite" => Ok(PathSecurity::HostInvite),
            "NftGated" => Ok(PathSecurity::NftGated),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AccessPermission {
    Table,
    Own,
    None,
}

#[derive(Debug, Serialize, Deserialize)]
/*
/m our@bedrock:bedrock:template.nec {"AddPath":{"target":"asdf", "data":{"path":{"path":"/asdf","host":"asdf","replication":"Host","access_rules":{},"security":"Public","metadata":{},"created_at":0,"updated_at":0,"received_at":0}}}}
*/
pub struct Path {
    pub path: String,
    pub host: String,
    pub replication: PathReplication,
    pub access_rules: HashMap<String, AccessRule>, // key is {tbl_name}-{role}
    pub security: PathSecurity,
    pub metadata: Value,
    pub created_at: u64,
    pub updated_at: u64,
    pub received_at: u64,
}
impl Path {
    fn to_value_vec(&self) -> Vec<Value> {
        print_to_terminal(0, &format!("{}",self.replication));
       // alway in the following order:
       // path, metadata, host, replication, security, created_at, updated_at, received_at
        vec![
            Value::String(self.path.clone()),
            Value::String(serde_json::to_string(&self.metadata).unwrap()),
            Value::String(self.host.clone()),
            Value::String(self.replication.to_string()),
            Value::String(self.security.to_string()),
            serde_json::to_value(self.created_at).unwrap(),
            serde_json::to_value(self.updated_at).unwrap(),
            serde_json::to_value(self.received_at).unwrap(),
        ]
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct SchemaCol(String, String);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowIdTriple {
    pub tbl_type: String,
    pub id: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub path: String,
    pub tbl_type: (String, String), // name, hash-of-schema
    pub id: (String, String), // tuple of PeerId and timestamp/whatever id system the peer used
    pub data: serde_json::map::Map<String, Value>, // JSON representing the row's data
    pub schema: Option<Vec<SchemaCol>>, // (column name, column SQL type)
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub received_at: SystemTime,
}
impl Row {
    pub fn tbl_name(&self) -> String {
        format!("{}_{}", self.tbl_type.0, self.tbl_type.1)
    }

    pub fn parse_tbl_name(tbl: String) -> (String, String) {
        let mut first = String::new();
        let last: String = tbl.split("_").last().unwrap().to_string();
        let mut i = 0;
        for part in tbl.split("_") {
            if part.to_string() == last {
                break;
            } else {
                if i == 0 {
                    first = part.to_string();
                } else {
                    first = format!("{}_{}", first, part);
                }
            }
            i += 1;
        }
        (first, last)
    }

    pub fn id_string(&self) -> String {
        format!("/{}{}", self.id.0, self.id.1)
    }
    pub fn parse_id(id: String) -> (String, String) {
        let mut first = String::new();
        let mut second = String::new();
        let mut i = 0;
        for part in id.split("/") {
            if i == 0 {
                // skip it
            } else if i == 1 {
                first = part.to_string();
            } else {
                second = format!("{}/{}", second, part);
            }
            i += 1;
        }
        (first, second)
    }
}
impl Default for Row {
    fn default() -> Self {
        Row {
            path: String::new(),
            tbl_type: (String::new(), String::new()),
            id: (String::new(), String::new()),
            data: serde_json::Map::new(),
            schema: None,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            received_at: SystemTime::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccessRule {
    pub path: String,
    pub tbl_type: String,
    pub role: String,
    pub create: bool,
    pub edit: AccessPermission,
    pub delete: AccessPermission,
}

#[derive(Debug, Serialize, Deserialize)]
struct Registration {
    peerid: String,
    pk: Vec<u8>,
    multiadr: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Confirmation {
    peerid: String,
    signed_nonce: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubRequest {
    pub path: Option<Path>,
    pub peers: Option<Vec<Peer>>,
    pub rows: Option<Vec<Row>>,
    pub ids: Option<Vec<RowIdTriple>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BedrockRequest {
    pub id: i32,
    pub kind: BedrockMessageType,
    pub target: String,
    pub data: SubRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BedrockResponse {
    pub id: i32,
    pub result: String,
}
/*
#[derive(NetworkBehaviour)]
pub struct MyBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub json: json::Behaviour<BedrockRequest, BedrockResponse>,
    pub rendezvous: rendezvous::client::Behaviour,
}

pub async fn start_swarm(db: &BedrockClient, port: Option<String>) -> Result<Swarm<MyBehaviour>, Error> {
    // 1. connect to and setup postgres tables (if they aren't already setup)
    // 2. generate/read-from-db a keypair + PeerId
    // 2b. create /{id} and /{id}/private paths if they aren't already there
    // ^^^ are done by BedrockClient::new("bedrock") which is passed in to this method

    // 3. start listening on an address via libp2p
    // 4. register that address with the PEER_SERVICE
    // 5. pull down + save all the peers from the PEER_SERVICE (into the all_peers table)
    // 6. dial all the peers who are hosts of db-paths we are a part of, to get any missing updates
    // Generate a random PeerId

    // 3. start listening on an address via libp2p
    let default_listen_str = String::from("/ip4/0.0.0.0/tcp/");
    let listen_str = match port {
        None => format!("{}{}", default_listen_str, "0"),
        Some(p) => format!("{}{}", default_listen_str, p),
    };
    print_to_terminal(0, "trying to start swarm");
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(db.key.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("tcp err")
        //.with_quic()
        .with_behaviour(|key| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build()
                .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(MyBehaviour {
                gossipsub,
                mdns,
                json: json::Behaviour::<BedrockRequest, BedrockResponse>::new(
                    [(
                        StreamProtocol::new("/my-json-protocol"),
                        rr::ProtocolSupport::Full,
                    )],
                    rr::Config::default(),
                ),
                rendezvous: rendezvous::client::Behaviour::new(key.clone()),
            })
        })
        .expect("behaviour error")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    swarm
        .listen_on(listen_str.parse().expect("asdf"))
        .unwrap();

    //let rendezvous_point_address = "/ip4/127.0.0.1/tcp/62649".parse::<Multiaddr>().unwrap();
    //swarm.dial(rendezvous_point_address.clone()).unwrap();

    let mut multiadr = String::new();
    //let rendezvous_point = "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN"
    //    .parse()
    //    .unwrap();
    match swarm.select_next_some().await {
        SwarmEvent::NewListenAddr { address, .. } => {
            println!("Local node is listening on {address}");
            multiadr = address.to_string();
            //let _ = swarm.behaviour_mut().rendezvous.register(
            //    rendezvous::Namespace::from_static("bedrock"),
            //    rendezvous_point,
            //    None,
            //);
        }
        _ => {}
    }
    println!("multiadr {multiadr}");
    let _ = match db
        .client
        .execute(
            "UPDATE self_info SET multiaddr = $1 WHERE id = $2",
            &[&multiadr, &db.peer_id.to_base58()],
        )
        .await
    {
        Ok(_) => println!("updated self_info"),
        Err(e) => println!("error updating self_info: {:?}", e),
    };
    // 5. pull down + save all the peers from the contract
    let peers = db.passport.get_peers().await.unwrap();
    // [PeerIdentity { name: "drunkplato", multiaddr: "/ip4/198.51.100.0/tcp/4242/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N" }, PeerIdentity { name: "tenari", multiaddr: "/ip4/127.0.0.1/tcp/60660" }]
    for peer in peers {
        println!("---> new peer: {:?}", peer);
        db.save_peer_connection_info(Some(peer.name), peer.peer_id, None).await;
        //let peerid = peer.multiaddr.split("/p2p/").last().unwrap();
        //println!("---> new peer: {} {}", peer.name, peerid);
        //let _ = db.client.execute(
        //    "INSERT INTO all_peers (name, id, addr) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET addr = EXCLUDED.addr",
        //    &[&peer.name, &peer.multiaddr],
        //).await;

        // for now, dial all the peers we know about, who aren't us
        // TODO: remove this and only dial peers we care about (hosts of db-paths)
        //if addr.len() > 0 && db.id.to_base58().as_str() != peerid.as_str() {
        //    let remote: Multiaddr = addr.parse().expect("bad multiaddr");
        //    println!("{:?}", remote);
        //    swarm.dial(remote).expect("bad dial");
        //    println!("Dialed {addr}")
        //}
    }

    // TODO:
    // 6. dial all the peers who are hosts of db-paths we are a part of, to get any missing updates

    Ok(swarm)
}

async fn connect(conn: &str) -> Result<tokio_postgres::Client, Error> {
//    let cert = fs::read("snakeoil.pem").unwrap();
//    let cert = Certificate::from_pem(&cert).unwrap();
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
//        .add_root_certificate(cert)
        .build().unwrap();
    let connector = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(conn, connector).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

*/
pub fn create_path(client: &Sqlite, our: &Address, path: Path) -> bool {
    let raw = client
        .read("SELECT host FROM paths WHERE path = ?;".to_string(), vec![Value::String(path.path.to_string())]);
    let raw = raw.unwrap();

    if raw.len() == 0 {
        let _ = client.write(
            String::from("INSERT INTO paths (path, metadata, host, replication, security, created_at, updated_at, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"),
            path.to_value_vec(),
            None
        );

        let _ = client.write(
           "INSERT INTO peers (path, id, role, metadata, created_at, updated_at, received_at) VALUES (?, ?, 'host', ?, ?, ?, ?)".to_string(),
            vec![Value::String(path.path), Value::String(our.to_string()), Value::String("{}".to_string()), serde_json::to_value(path.created_at).unwrap(), serde_json::to_value(path.updated_at).unwrap(), serde_json::to_value(path.received_at).unwrap()],
            None
        );
        true
    } else {
        false
    }
}

/*
#[derive(Debug, Serialize, Deserialize)]
pub struct ChatDbScry {
    pub tables: ChatDbTables,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatDbTables {
    pub messages: Vec<ChatDbMessage>,
    pub paths: Vec<ChatDbPath>,
    pub peers: Vec<ChatDbPeer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatDbMessage {
    #[serde(rename = "received-at")]
    pub received_at: u64,
    pub metadata: Value,
    #[serde(rename = "reply-to")]
    pub reply_to: Option<ChatDbReplyTo>,
    #[serde(rename = "updated-at")]
    pub updated_at: u64,
    #[serde(rename = "msg-part-id")]
    pub msg_part_id: u8,
    #[serde(rename = "created-at")]
    pub created_at: u64,
    pub path: String,
    #[serde(rename = "content-data")]
    pub content_data: String,
    #[serde(rename = "expires-at")]
    pub expires_at: Option<u64>,
    pub sender: String,
    #[serde(rename = "content-type")]
    pub content_type: String,
    #[serde(rename = "msg-id")]
    pub msg_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatDbReplyTo {
    #[serde(rename = "msg-id")]
    pub msg_id: String,
    pub path: String,
}

pub fn chat_db_message_def() -> Vec<SchemaCol> {
    vec![
        SchemaCol(String::from("metadata"), String::from("json")),
        SchemaCol(String::from("reply_to"), String::from("json")),
        SchemaCol(String::from("expires_at"), String::from("timestamp")),
        SchemaCol(String::from("content_data"), String::from("text")),
        SchemaCol(String::from("content_type"), String::from("text")),
    ]
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatDbPath {
    #[serde(rename = "created-at")]
    pub created_at: u64,
    #[serde(rename = "received-at")]
    pub received_at: u64,
    #[serde(rename = "updated-at")]
    pub updated_at: u64,
    pub metadata: Value,
    pub path: String,
    #[serde(rename = "max-expires-at-duration")]
    pub max_expires_at_duration: u64,
    pub invites: String,
    pub pins: Vec<String>,
    #[serde(rename = "peers-get-backlog")]
    pub peers_get_backlog: bool,
    pub nft: Value,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatDbPeer {
    #[serde(rename = "created-at")]
    pub created_at: u64,
    #[serde(rename = "received-at")]
    pub received_at: u64,
    #[serde(rename = "updated-at")]
    pub updated_at: u64,
    pub path: String,
    pub role: String,
    pub ship: String,
}

*/
fn table_name_helper(client: &Sqlite, table: String) -> Result<(), ()> {
    let tbl_names = client
        .read(
            "select table_name from information_schema.tables where table_schema = 'public';".to_string(),
            vec![],
        )
        .unwrap();
    let mut found_name = false;
    for name_row in tbl_names {
        let name: String = name_row.get("table_name").unwrap().as_str().unwrap().to_string();
        if name == table {
            found_name = true;
        }
    }
    if !found_name {
        return Err(());
    }
    Ok(())
}

pub fn get_by_id(client: &Sqlite, table: String, id: String) -> Result<Row, ()> {
    let table_good = table_name_helper(&client, table.clone());
    if let Err(_) = table_good {
        return Err(());
    }

    let raw = client
        .read(format!("SELECT * FROM {} WHERE id = $1", table), vec![Value::String(id)])
        .unwrap();

    Ok(parse_row(&raw[0], table))
}

pub fn get_by_id2(client: &Sqlite, table: String, id: String) -> Result<Row, ()> {
    let table_good = table_name_helper(client, table.clone());
    if let Err(_) = table_good {
        return Err(());
    }

    let raw = client
        .read(format!("SELECT * FROM {} WHERE id = $1", table), vec![Value::String(id)])
        .unwrap();

    Ok(parse_row(&raw[0], table))
}

pub fn get_peer(client: &Sqlite, path: String, id: String) -> Result<Peer, ()> {
    let raw = client
        .read(
            String::from("SELECT * FROM peers WHERE path = $1 AND id = $2"),
            vec![Value::String(path), Value::String(id)]
        );
    match raw {
        Err(_) => Err(()),
        Ok(raw) => {
            if raw.len() == 0 {
                Err(())
            } else {
                let peer = Peer {
                    path: raw[0].get("path").unwrap().as_str().unwrap().to_string(),
                    id: raw[0].get("id").unwrap().as_str().unwrap().to_string(),
                    role: raw[0].get("role").unwrap().as_str().unwrap().to_string(),
                    metadata: raw[0].get("metadata").unwrap().clone(),
                    created_at: value_to_sys(raw[0].get("created_at").unwrap()),
                    updated_at: value_to_sys(raw[0].get("updated_at").unwrap()),
                    received_at: value_to_sys(raw[0].get("received_at").unwrap()),
                };
                Ok(peer)
            }
        }
    }
}

pub fn get_table(client: &Sqlite, table: String, path: String) -> Result<Vec<Row>, ()> {
    let table_good = table_name_helper(&client, table.clone());
    if let Err(_) = table_good {
        return Err(());
    }

    let raw = client
        .read(
            format!("SELECT * FROM {} WHERE path = $1", table),
            vec![Value::String(path)],
        )
        .unwrap();

    let mut results: Vec<Row> = Vec::with_capacity(raw.len());

    for r in raw {
        results.push(parse_row(&r, table.clone()));
    }

    Ok(results)
}

pub fn get_fullpath(client: &Sqlite, path: String) -> Result<Vec<Row>, ()> {
    let tbl_names = client
        .read(
            String::from("select table_name from information_schema.tables where table_schema = 'public';"),
            vec![],
        )
        .unwrap();
    let mut results: Vec<Row> = Vec::with_capacity(tbl_names.len());
    for name_row in tbl_names {
        let table = name_row.get("table_name").unwrap().as_str().unwrap();
        if DEFAULT_TABLES.contains(&table) {
            continue;
        } else {
            let raw = client
                .read(
                    format!("SELECT * FROM {} WHERE path = $1", table),
                    vec![Value::String(path.clone())],
                )
                .unwrap();
            for r in raw {
                results.push(parse_row(&r, table.to_string()));
            }
        }
    }

    Ok(results)
}

pub fn get_fulltable(client: &Sqlite, table: String) -> Result<Vec<Row>, ()> {
    let table_good = table_name_helper(&client, table.clone());
    if let Err(_) = table_good {
        return Err(());
    }

    let raw = client
        .read(format!("SELECT * FROM {}", table), vec![])
        .unwrap();

    let mut results: Vec<Row> = Vec::with_capacity(raw.len());
    for r in raw {
        results.push(parse_row(&r, table.clone()));
    }

    Ok(results)
}

/*
pub async fn get_chat_db(conn: &str) -> Result<ChatDbScry, ()> {
    let client = connect(conn).await.unwrap();
    let raw = client
        .query("SELECT * FROM message_13142003246720374445", &[])
        .await
        .unwrap();

    let mut results = ChatDbScry {
        tables: ChatDbTables {
            paths: vec![],
            messages: Vec::with_capacity(raw.len()),
            peers: vec![],
        }
    };
    for r in raw {
        let path: String = r.get("path");
        let id: String = r.get("id");
        let mut msg_id = String::new();
        for p in id.split("/").skip(2).take(2) {
            msg_id.push('/');
            msg_id.push_str(p);
        }
        let msg_part_id: u8 = id.split("/").last().unwrap().parse().unwrap();
        let sender: String = id.split("/").skip(3).next().unwrap().to_string();
        let reply: Option<Value> = r.get("reply_to");
        let reply_to: Option<ChatDbReplyTo> = match reply {
            None => None,
            Some(v) => {
                if let Value::String(mid) = v.get("msg_id").unwrap() {
                    if let Value::String(path) = v.get("path").unwrap() {
                        Some(ChatDbReplyTo {
                            msg_id: mid.to_string(),
                            path: path.to_string(),
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };
        let c_at: SystemTime = r.get("created_at");
        let u_at: SystemTime = r.get("updated_at");
        let r_at: SystemTime = r.get("received_at");
        let e_at: Option<SystemTime> = r.get("expires_at");
        results.tables.messages.push(ChatDbMessage {
            path: path.clone(),
            msg_part_id,
            msg_id,
            content_type: r.get("content_type"),
            content_data: r.get("content_data"),
            sender,
            created_at: c_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(),
            updated_at: u_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(),
            received_at: r_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(),
            expires_at: match e_at {
                None => None,
                Some(e) => Some(e.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap()),
            },
            metadata: r.get("metadata"),
            reply_to,
        });

        let mut matching_path = false;
        for p in results.tables.paths.clone() {
            if p.path == path {
                matching_path = true;
            }
        }

        if !matching_path {
            let pat = get_path(&client, &path).await;
            if let Ok(p) = pat {
                let mut pins: Vec<String> = Vec::new();
                for pin in p.metadata.as_object().unwrap().get("pins").unwrap().as_array().unwrap() {
                    pins.push(pin.as_str().unwrap().to_string());
                }
                results.tables.paths.push(ChatDbPath {
                    pins,
                    peers_get_backlog: p.metadata.as_object().unwrap().get("peers_get_backlog").unwrap().as_bool().unwrap(),
                    max_expires_at_duration: p.metadata.as_object().unwrap().get("max_expires_at_duration").unwrap().as_u64().unwrap(),
                    type_: p.metadata.as_object().unwrap().get("type_").unwrap().as_str().unwrap().to_string(),
                    path: p.path,
                    metadata: p.metadata,
                    nft: match p.security {
                        PathSecurity::NftGated => json!(true),
                        _ => json!(null),
                    },
                    invites: match p.security {
                        PathSecurity::HostInvite => "host",
                        PathSecurity::MemberInvite => "anyone",
                        PathSecurity::Public => "open",
                        PathSecurity::NftGated => "open",
                    }.to_string(),
                    created_at: p.created_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(),
                    updated_at: p.updated_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(),
                    received_at: p.received_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(),
                });
            }
        }
    }

    Ok(results)
}

*/
fn parse_row(r: &HashMap<String, serde_json::Value>, table: String) -> Row {
    let tbl = table.split("_").collect::<Vec<&str>>();
    let tbl = (tbl[0].to_string(), tbl[1].to_string());

    let id: String = r.get("id").unwrap().as_str().unwrap().to_string();
    let id_parts = id.split("/").collect::<Vec<&str>>();
    let mut data = serde_json::Map::new();
    for cname in r.keys() {
        if cname == "created_at"
            || cname == "updated_at"
            || cname == "path"
            || cname == "id"
            || cname == "received_at"
        {
            continue;
        }
        match r.get(cname) {
            Some(val) => {
                data.insert(cname.clone(), val.clone());
            }
            None => {
                data.insert(cname.clone(), Value::Null);
            }
        }
    }

    Row {
        path: r.get("path").unwrap().as_str().unwrap().to_string(),
        tbl_type: tbl,
        id: (id_parts[1].to_string(), id_parts[2..].join("/")),
        schema: None,
        created_at: value_to_sys(r.get("created_at").unwrap()),
        updated_at: value_to_sys(r.get("updated_at").unwrap()),
        received_at: value_to_sys(r.get("received_at").unwrap()),
        data,
    }
}

pub fn peers_for(path: &str, client: &Sqlite) -> Vec<String> {
    let raw = client
        .read(
            String::from("SELECT id FROM peers WHERE path = $1"),
            vec![Value::String(String::from(path))]
        )
        .unwrap();
    let mut result: Vec<String> = Vec::with_capacity(raw.len());
    for row in raw {
        result.push(row.get("id").unwrap().as_str().unwrap().to_string());
    }
    result
}

fn we_are_in_path(path: &str, client: &Sqlite) -> bool {
    let raw = client
        .read("SELECT host FROM paths WHERE path = $1".to_string(), vec![Value::String(path.to_string())])
        .unwrap();

    if raw.len() == 0 {
        false
    } else {
        true
    }
}

pub fn get_path(client: &Sqlite, path: &str) -> Result<Path, ()> {
    let raw = client
        .read("SELECT * FROM paths WHERE path = $1".to_string(), vec![Value::String(path.to_string())])
        .unwrap();

    if raw.len() == 0 {
        Err(())
    } else {
        Ok(Path {
            path: raw[0].get("path").unwrap().as_str().unwrap().to_string(),
            host: raw[0].get("host").unwrap().as_str().unwrap().to_string(),
            metadata: raw[0].get("metadata").unwrap().clone(),
            replication: PathReplication::from_str(raw[0].get("replication").unwrap().as_str().unwrap()).unwrap(),
            // TODO: actually read from the access_rules table and generate this properly
            access_rules: HashMap::new(),
            security: PathSecurity::from_str(raw[0].get("security").unwrap().as_str().unwrap()).unwrap(),
            created_at: raw[0].get("created_at").unwrap().as_u64().unwrap(),
            updated_at: raw[0].get("updated_at").unwrap().as_u64().unwrap(),
            received_at: raw[0].get("received_at").unwrap().as_u64().unwrap(),
        })
    }
}

fn value_to_sys(time: &Value) -> SystemTime {
     let dur = Duration::from_millis(time.as_u64().unwrap());
     SystemTime::UNIX_EPOCH.checked_add(dur).unwrap()
}

fn sys_time_to_u64(time: SystemTime) -> u64 {
    let num: u64 = time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().try_into().unwrap();
    num
}

fn sys_time_to_value(time: SystemTime) -> Value {
    let num: u64 = sys_time_to_u64(time);
    serde_json::to_value(serde_json::value::Number::from(num)).unwrap()
}

