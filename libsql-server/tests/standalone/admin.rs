use std::path::PathBuf;
use std::time::Duration;

use hyper::StatusCode;
use libsql_server::config::{AdminApiConfig, RpcServerConfig, UserApiConfig};
use s3s::header::AUTHORIZATION;
use serde_json::json;
use tempfile::tempdir;
use turmoil::Sim;

use crate::common::{
    http::Client,
    net::{init_tracing, SimServer as _, TestServer, TurmoilAcceptor, TurmoilConnector},
};

fn make_primary(sim: &mut Sim, path: PathBuf) {
    init_tracing();
    sim.host("primary", move || {
        let path = path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                user_api_config: UserApiConfig {
                    ..Default::default()
                },
                admin_api_config: Some(AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: true,
                    auth_key: None,
                }),
                rpc_server_config: Some(RpcServerConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                    tls_config: None,
                }),
                disable_namespaces: false,
                disable_default_namespace: false,
                ..Default::default()
            };

            server.start_sim(8080).await?;

            Ok(())
        }
    });
}

#[test]
fn admin_auth() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", || async move {
        let tmp = tempdir().unwrap();
        let server = TestServer {
            path: tmp.path().to_owned().into(),
            user_api_config: UserApiConfig {
                hrana_ws_acceptor: None,
                ..Default::default()
            },
            admin_api_config: Some(AdminApiConfig {
                acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                connector: TurmoilConnector,
                disable_metrics: true,
                auth_key: Some("secretkey".into()),
            }),
            disable_namespaces: false,
            ..Default::default()
        };
        server.start_sim(8080).await?;
        Ok(())
    });

    sim.client("test", async {
        let client = Client::new();

        assert_eq!(
            client
                .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert!(client
            .post_with_headers(
                "http://primary:9090/v1/namespaces/foo/create",
                &[(AUTHORIZATION, "basic  secretkey")],
                json!({})
            )
            .await
            .unwrap()
            .status()
            .is_success());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn list_namespaces_basic() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();

        // Step 1: List initially - should have no namespaces (default is not auto-created when namespaces are enabled)
        let resp = client.get("http://primary:9090/v1/namespaces").await?;
        assert!(resp.status().is_success());

        let body: serde_json::Value = resp.json().await?;
        let namespaces = body["namespaces"].as_array().unwrap();
        assert_eq!(namespaces.len(), 0);

        // Step 2: Create default namespace explicitly
        client
            .post(
                "http://primary:9090/v1/namespaces/default/create",
                json!({}),
            )
            .await?;

        // Step 3: Create foo namespace
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        // Step 4: Create schema namespace and bar with shared_schema_name
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({ "shared_schema": true }),
            )
            .await?;
        client
            .post(
                "http://primary:9090/v1/namespaces/bar/create",
                json!({ "shared_schema_name": "schema" }),
            )
            .await?;

        // Step 5: List again - should have 4 namespaces (default, foo, bar, schema)
        let resp = client.get("http://primary:9090/v1/namespaces").await?;
        let body: serde_json::Value = resp.json().await?;
        let namespaces = body["namespaces"].as_array().unwrap();
        assert_eq!(namespaces.len(), 4);

        // Verify all namespace names are present
        let names: Vec<_> = namespaces
            .iter()
            .map(|n| n["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"default"));
        assert!(names.contains(&"foo"));
        assert!(names.contains(&"bar"));
        assert!(names.contains(&"schema"));

        // Verify shared_schema_name for bar
        let bar = namespaces.iter().find(|n| n["name"] == "bar").unwrap();
        assert_eq!(bar["shared_schema_name"], "schema");

        // Verify foo doesn't have shared_schema_name
        let foo = namespaces.iter().find(|n| n["name"] == "foo").unwrap();
        assert!(foo["shared_schema_name"].is_null());

        Ok(())
    });

    sim.run().unwrap();
}
