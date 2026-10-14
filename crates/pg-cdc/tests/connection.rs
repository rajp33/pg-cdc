use aws_config::{BehaviorVersion, Region};
use aws_sdk_rds::config::Credentials;
use pg_cdc::config::TlsMode;
use pg_cdc::connection::{
    ConnectParams, ConnectionProvider, PasswordProvider, RdsIamProvider, TlsConfig, connect_control,
};
use pg_cdc_testkit::{Postgres, PostgresOptions};
use secrecy::{ExposeSecret, SecretString};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn params(pg: &Postgres, mode: TlsMode) -> ConnectParams {
    ConnectParams {
        host: pg.host().to_owned(),
        port: pg.port(),
        database: pg.database().to_owned(),
        user: pg.user().to_owned(),
        password: Some(SecretString::from(pg.password().to_owned())),
        tls: TlsConfig {
            mode,
            ca_file: pg.ca_file().map(std::path::Path::to_path_buf),
        },
    }
}

#[tokio::test]
async fn password_provider_returns_its_params_every_time() -> TestResult {
    let base = ConnectParams {
        host: "db.internal".to_owned(),
        port: 6432,
        database: "app".to_owned(),
        user: "pgcdc".to_owned(),
        password: Some(SecretString::from("secret".to_owned())),
        tls: TlsConfig {
            mode: TlsMode::Require,
            ca_file: None,
        },
    };
    let provider = PasswordProvider::new(base);
    for _ in 0..2 {
        let params = provider.params().await?;
        assert_eq!(
            (params.host.as_str(), params.port, params.user.as_str()),
            ("db.internal", 6432, "pgcdc")
        );
        assert_eq!(
            params.password.as_ref().map(ExposeSecret::expose_secret),
            Some("secret")
        );
        assert_eq!(params.tls.mode, TlsMode::Require);
    }
    Ok(())
}

#[tokio::test]
async fn rds_iam_provider_signs_a_fresh_connect_token() -> TestResult {
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI",
            None,
            None,
            "test",
        ))
        .load()
        .await;
    let base = ConnectParams {
        host: "db.cluster-xyz.us-east-1.rds.amazonaws.com".to_owned(),
        port: 5432,
        database: "app".to_owned(),
        user: "pgcdc".to_owned(),
        password: None,
        tls: TlsConfig {
            mode: TlsMode::VerifyFull,
            ca_file: None,
        },
    };
    let provider = RdsIamProvider::new(base, sdk);
    let params = provider.params().await?;
    let token = params
        .password
        .as_ref()
        .ok_or("no token")?
        .expose_secret()
        .to_owned();
    assert!(
        token.starts_with(
            "db.cluster-xyz.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=pgcdc&"
        ),
        "{token}"
    );
    for part in [
        "X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "X-Amz-Credential=AKIDEXAMPLE",
        "us-east-1",
        "X-Amz-Expires=900",
        "X-Amz-Signature=",
    ] {
        assert!(token.contains(part), "{part} missing from {token}");
    }
    assert!(!token.contains("wJalrXUtnFEMI"));
    assert_eq!(
        (params.host.as_str(), params.port, params.database.as_str()),
        ("db.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, "app")
    );
    assert!(!format!("{params:?}").contains("X-Amz-Signature"));
    Ok(())
}

#[tokio::test]
async fn control_connection_pins_session_settings() -> TestResult {
    let pg = Postgres::start().await?;
    for setting in [
        "ALTER DATABASE postgres SET timezone = 'America/New_York'",
        "ALTER DATABASE postgres SET datestyle = 'SQL, DMY'",
        "ALTER DATABASE postgres SET intervalstyle = 'iso_8601'",
        "ALTER DATABASE postgres SET bytea_output = 'escape'",
        "ALTER DATABASE postgres SET extra_float_digits = 0",
    ] {
        pg.execute(setting).await?;
    }
    let control = connect_control(&params(&pg, TlsMode::Disable)).await?;
    for (setting, expected) in [
        ("TimeZone", "UTC"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("bytea_output", "hex"),
        ("extra_float_digits", "1"),
        ("application_name", "pg-cdc"),
    ] {
        let value: String = control
            .client()
            .query_one(&format!("SHOW {setting}"), &[])
            .await?
            .try_get(0)?;
        assert_eq!(value, expected, "{setting}");
    }
    Ok(())
}

#[tokio::test]
async fn connects_with_special_character_password() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute(r#"CREATE ROLE "pg cdc" LOGIN PASSWORD 'p@ss w:rd/''"&=%'"#)
        .await?;
    let mut params = params(&pg, TlsMode::Disable);
    params.user = "pg cdc".to_owned();
    params.password = Some(SecretString::from(r#"p@ss w:rd/'"&=%"#.to_owned()));
    let control = connect_control(&params).await?;
    let user: String = control
        .client()
        .query_one("SELECT current_user::text", &[])
        .await?
        .try_get(0)?;
    assert_eq!(user, "pg cdc");
    Ok(())
}

async fn ssl_in_use(
    params: &ConnectParams,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let control = connect_control(params).await?;
    let row = control
        .client()
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await?;
    Ok(row.try_get(0)?)
}

#[tokio::test]
async fn tls_modes_encrypt_and_verify_as_configured() -> TestResult {
    let pg = Postgres::start_with(PostgresOptions { tls: true }).await?;

    let mut require = params(&pg, TlsMode::Require);
    require.tls.ca_file = None;
    assert!(ssl_in_use(&require).await?);

    let mut verify_full = params(&pg, TlsMode::VerifyFull);
    verify_full.host = "localhost".to_owned();
    assert!(ssl_in_use(&verify_full).await?);

    let mut wrong_host = params(&pg, TlsMode::VerifyFull);
    wrong_host.host = "127.0.0.1".to_owned();
    assert!(
        connect_control(&wrong_host).await.is_err(),
        "verify-full must check the host name"
    );

    let verify_ca = params(&pg, TlsMode::VerifyCa);
    assert_eq!(verify_ca.host, "127.0.0.1");
    assert!(ssl_in_use(&verify_ca).await?);

    let mut unknown_ca = params(&pg, TlsMode::VerifyFull);
    unknown_ca.host = "localhost".to_owned();
    unknown_ca.tls.ca_file = None;
    assert!(
        connect_control(&unknown_ca).await.is_err(),
        "the public roots must not trust the test CA"
    );

    let disabled = params(&pg, TlsMode::Disable);
    assert!(!ssl_in_use(&disabled).await?);
    Ok(())
}

#[tokio::test]
async fn missing_ca_file_is_a_clear_error() -> TestResult {
    let pg = Postgres::start().await?;
    let mut params = params(&pg, TlsMode::VerifyFull);
    params.tls.ca_file = Some("/nonexistent/ca.pem".into());
    let error = connect_control(&params)
        .await
        .err()
        .ok_or("expected an error")?;
    assert!(error.to_string().contains("/nonexistent/ca.pem"), "{error}");
    Ok(())
}
