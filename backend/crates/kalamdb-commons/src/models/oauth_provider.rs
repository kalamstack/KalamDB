//! Well-known OAuth / OIDC identity providers.
//!
//! Each provider has a deterministic 3-character prefix used in
//! `oidc:{prefix}:{subject}` usernames. This lives in `kalamdb-commons`
//! so that shared auth models and system auth data can
//! reference it without circular dependencies.

use std::fmt;

/// Well-known OAuth / OIDC identity providers.
///
/// Serialises as a lowercase snake_case string.  Unknown provider strings
/// deserialize into [`Custom`](OAuthProvider::Custom) so that new providers
/// can be added at the identity-provider side without requiring a KalamDB
/// code change.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OAuthProvider {
    /// Keycloak (any realm)
    Keycloak,
    /// Google / Google Workspace
    Google,
    /// Microsoft Azure Active Directory / Entra ID
    AzureAd,
    /// Amazon Cognito
    Cognito,
    /// AWS IAM Identity Center (successor to AWS SSO)
    AwsIam,
    /// GitHub OAuth / GitHub Apps
    GitHub,
    /// GitLab (self-hosted or gitlab.com)
    GitLab,
    /// Meta / Facebook Login
    Facebook,
    /// X (formerly Twitter)
    X,
    /// Sign in with Apple
    Apple,
    /// Firebase Authentication (backed by Google Identity Platform)
    Firebase,
    /// Okta / Okta Workforce Identity Cloud
    Okta,
    /// Auth0 (by Okta)
    Auth0,
    /// Supabase Auth (GoTrue-based)
    Supabase,
    /// OneLogin
    OneLogin,
    /// Ping Identity / PingFederate
    PingIdentity,
    /// Salesforce Identity
    Salesforce,
    /// Oracle Identity Cloud Service
    Oracle,
    /// IBM Security Verify
    Ibm,
    /// JumpCloud
    JumpCloud,
    /// Duo Security
    Duo,
    /// FusionAuth
    FusionAuth,
    /// Authentik (open-source)
    Authentik,
    /// Zitadel (open-source)
    Zitadel,
    /// Casdoor (open-source)
    Casdoor,
    /// Logto (open-source)
    Logto,
    /// Clerk
    Clerk,
    /// Stytch
    Stytch,
    /// WorkOS
    WorkOS,
    /// Descope
    Descope,
    /// Any provider not in the well-known list.
    /// The contained string is the raw provider identifier.
    Custom(String),
}

impl OAuthProvider {
    /// Canonical string representation (matches the serde serialisation).
    pub fn as_str(&self) -> &str {
        match self {
            Self::Keycloak => "keycloak",
            Self::Google => "google",
            Self::AzureAd => "azure_ad",
            Self::Cognito => "cognito",
            Self::AwsIam => "aws_iam",
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
            Self::Facebook => "facebook",
            Self::X => "x",
            Self::Apple => "apple",
            Self::Firebase => "firebase",
            Self::Okta => "okta",
            Self::Auth0 => "auth0",
            Self::Supabase => "supabase",
            Self::OneLogin => "onelogin",
            Self::PingIdentity => "ping_identity",
            Self::Salesforce => "salesforce",
            Self::Oracle => "oracle",
            Self::Ibm => "ibm",
            Self::JumpCloud => "jumpcloud",
            Self::Duo => "duo",
            Self::FusionAuth => "fusionauth",
            Self::Authentik => "authentik",
            Self::Zitadel => "zitadel",
            Self::Casdoor => "casdoor",
            Self::Logto => "logto",
            Self::Clerk => "clerk",
            Self::Stytch => "stytch",
            Self::WorkOS => "workos",
            Self::Descope => "descope",
            Self::Custom(s) => s.as_str(),
        }
    }

    /// 3-character prefix used in `oidc:{prefix}:{subject}` usernames.
    ///
    /// Well-known providers get a deterministic static prefix.
    /// Custom providers get the first 3 hex characters of the SHA-256 hash
    /// of their identifier string.
    pub fn prefix(&self) -> String {
        let static_prefix = match self {
            Self::Keycloak => "kcl",
            Self::Google => "ggl",
            Self::AzureAd => "msf",
            Self::Cognito => "cgn",
            Self::AwsIam => "aws",
            Self::GitHub => "ghb",
            Self::GitLab => "glb",
            Self::Facebook => "fbk",
            Self::X => "xco",
            Self::Apple => "apl",
            Self::Firebase => "fbs",
            Self::Okta => "okt",
            Self::Auth0 => "a0x",
            Self::Supabase => "sbs",
            Self::OneLogin => "olg",
            Self::PingIdentity => "png",
            Self::Salesforce => "sfc",
            Self::Oracle => "orc",
            Self::Ibm => "ibm",
            Self::JumpCloud => "jcl",
            Self::Duo => "duo",
            Self::FusionAuth => "fsa",
            Self::Authentik => "atk",
            Self::Zitadel => "zit",
            Self::Casdoor => "csd",
            Self::Logto => "lgt",
            Self::Clerk => "clk",
            Self::Stytch => "sty",
            Self::WorkOS => "wos",
            Self::Descope => "dsc",
            Self::Custom(s) => {
                use sha2::Digest;
                let hash = hex::encode(sha2::Sha256::digest(s.as_bytes()));
                return hash[..3].to_string();
            },
        };
        static_prefix.to_string()
    }

    /// Reverse-lookup a provider from its 3-character username prefix.
    ///
    /// Unknown prefixes return [`Custom`](Self::Custom) with the prefix
    /// as the identifier (lossy — the original issuer URL is not recoverable
    /// from a hash prefix).
    pub fn from_prefix(prefix: &str) -> Self {
        match prefix {
            "kcl" => Self::Keycloak,
            "ggl" => Self::Google,
            "msf" => Self::AzureAd,
            "cgn" => Self::Cognito,
            "aws" => Self::AwsIam,
            "ghb" => Self::GitHub,
            "glb" => Self::GitLab,
            "fbk" => Self::Facebook,
            "xco" => Self::X,
            "apl" => Self::Apple,
            "fbs" => Self::Firebase,
            "okt" => Self::Okta,
            "a0x" => Self::Auth0,
            "sbs" => Self::Supabase,
            "olg" => Self::OneLogin,
            "png" => Self::PingIdentity,
            "sfc" => Self::Salesforce,
            "orc" => Self::Oracle,
            "ibm" => Self::Ibm,
            "jcl" => Self::JumpCloud,
            "duo" => Self::Duo,
            "fsa" => Self::FusionAuth,
            "atk" => Self::Authentik,
            "zit" => Self::Zitadel,
            "csd" => Self::Casdoor,
            "lgt" => Self::Logto,
            "clk" => Self::Clerk,
            "sty" => Self::Stytch,
            "wos" => Self::WorkOS,
            "dsc" => Self::Descope,
            other => Self::Custom(other.to_string()),
        }
    }

    /// Parse from a string, falling back to [`Custom`](Self::Custom) for
    /// unknown values.
    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "keycloak" => Self::Keycloak,
            "google" => Self::Google,
            "azure_ad" | "azure" | "microsoft" => Self::AzureAd,
            "cognito" | "aws_cognito" => Self::Cognito,
            "aws_iam" => Self::AwsIam,
            "github" => Self::GitHub,
            "gitlab" => Self::GitLab,
            "facebook" | "meta" => Self::Facebook,
            "x" | "twitter" => Self::X,
            "apple" => Self::Apple,
            "firebase" | "google_identity_platform" => Self::Firebase,
            "okta" => Self::Okta,
            "auth0" => Self::Auth0,
            "supabase" => Self::Supabase,
            "onelogin" => Self::OneLogin,
            "ping_identity" | "ping" | "pingfederate" => Self::PingIdentity,
            "salesforce" => Self::Salesforce,
            "oracle" => Self::Oracle,
            "ibm" => Self::Ibm,
            "jumpcloud" => Self::JumpCloud,
            "duo" => Self::Duo,
            "fusionauth" => Self::FusionAuth,
            "authentik" => Self::Authentik,
            "zitadel" => Self::Zitadel,
            "casdoor" => Self::Casdoor,
            "logto" => Self::Logto,
            "clerk" => Self::Clerk,
            "stytch" => Self::Stytch,
            "workos" => Self::WorkOS,
            "descope" => Self::Descope,
            other => Self::Custom(other.to_string()),
        }
    }

    /// Detect the provider type from an OIDC issuer URL.
    ///
    /// Uses substring matching on well-known issuer URL patterns.
    /// Falls back to [`Custom`](Self::Custom) with the raw URL.
    pub fn detect_from_issuer(issuer: &str) -> Self {
        let lower = issuer.to_lowercase();

        if lower.contains("keycloak") || lower.contains("/realms/") {
            return Self::Keycloak;
        }
        if lower.contains("accounts.google.com") {
            return Self::Google;
        }
        if lower.contains("login.microsoftonline.com") || lower.contains("sts.windows.net") {
            return Self::AzureAd;
        }
        if lower.contains("cognito-idp") && lower.contains("amazonaws.com") {
            return Self::Cognito;
        }
        if lower.contains("github.com") {
            return Self::GitHub;
        }
        if lower.contains("gitlab.com") || lower.contains("gitlab") {
            return Self::GitLab;
        }
        if lower.contains("facebook.com") {
            return Self::Facebook;
        }
        if lower.contains("appleid.apple.com") {
            return Self::Apple;
        }
        if lower.contains("securetoken.google.com") {
            return Self::Firebase;
        }
        if lower.contains("okta.com") || lower.contains("oktapreview.com") {
            return Self::Okta;
        }
        if lower.contains("auth0.com") {
            return Self::Auth0;
        }
        if lower.contains("supabase") {
            return Self::Supabase;
        }
        if lower.contains("onelogin.com") {
            return Self::OneLogin;
        }
        if lower.contains("pingidentity.com") || lower.contains("pingone.com") {
            return Self::PingIdentity;
        }
        if lower.contains("salesforce.com") || lower.contains("force.com") {
            return Self::Salesforce;
        }
        if lower.contains("identity.oraclecloud.com") {
            return Self::Oracle;
        }
        if lower.contains("verify.ibm.com") {
            return Self::Ibm;
        }
        if lower.contains("jumpcloud.com") {
            return Self::JumpCloud;
        }
        if lower.contains("duosecurity.com") {
            return Self::Duo;
        }
        if lower.contains("fusionauth") {
            return Self::FusionAuth;
        }
        if lower.contains("authentik") {
            return Self::Authentik;
        }
        if lower.contains("zitadel") {
            return Self::Zitadel;
        }
        if lower.contains("casdoor") {
            return Self::Casdoor;
        }
        if lower.contains("logto") {
            return Self::Logto;
        }
        if lower.contains("clerk") {
            return Self::Clerk;
        }
        if lower.contains("stytch.com") {
            return Self::Stytch;
        }
        if lower.contains("workos.com") {
            return Self::WorkOS;
        }
        if lower.contains("descope.com") {
            return Self::Descope;
        }
        // Twitter/X doesn't have a standard OIDC issuer, but handle it
        if lower.contains("twitter.com") || lower.contains("x.com") {
            return Self::X;
        }

        Self::Custom(issuer.to_string())
    }
}

impl fmt::Display for OAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Conditional serde impls (behind "serde" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "serde")]
mod serde_impl {
    use super::OAuthProvider;

    impl serde::Serialize for OAuthProvider {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(self.as_str())
        }
    }

    impl<'de> serde::Deserialize<'de> for OAuthProvider {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            Ok(Self::from_str_lossy(&s))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth_provider_serde_roundtrip() {
        for provider in &[
            OAuthProvider::Keycloak,
            OAuthProvider::Google,
            OAuthProvider::AzureAd,
            OAuthProvider::GitHub,
            OAuthProvider::Auth0,
            OAuthProvider::Custom("my_idp".to_string()),
        ] {
            let json = serde_json::to_string(provider).unwrap();
            let back: OAuthProvider = serde_json::from_str(&json).unwrap();
            assert_eq!(*provider, back, "round-trip failed for {json}");
        }
    }

    #[test]
    fn test_oauth_provider_detect_keycloak() {
        let p = OAuthProvider::detect_from_issuer("https://keycloak.example.com/realms/myrealm");
        assert_eq!(p, OAuthProvider::Keycloak);
    }

    #[test]
    fn test_oauth_provider_detect_google() {
        let p = OAuthProvider::detect_from_issuer("https://accounts.google.com");
        assert_eq!(p, OAuthProvider::Google);
    }

    #[test]
    fn test_oauth_provider_detect_azure() {
        let p =
            OAuthProvider::detect_from_issuer("https://login.microsoftonline.com/tenant-id/v2.0");
        assert_eq!(p, OAuthProvider::AzureAd);
    }

    #[test]
    fn test_oauth_provider_detect_custom() {
        let p = OAuthProvider::detect_from_issuer("https://my-idp.internal.corp");
        assert_eq!(p, OAuthProvider::Custom("https://my-idp.internal.corp".to_string()));
    }

    #[test]
    fn test_oauth_provider_aliases() {
        assert_eq!(OAuthProvider::from_str_lossy("twitter"), OAuthProvider::X);
        assert_eq!(OAuthProvider::from_str_lossy("meta"), OAuthProvider::Facebook);
        assert_eq!(OAuthProvider::from_str_lossy("microsoft"), OAuthProvider::AzureAd);
        assert_eq!(OAuthProvider::from_str_lossy("aws_cognito"), OAuthProvider::Cognito);
    }

    #[test]
    fn test_prefix_roundtrip_wellknown() {
        let providers = [
            OAuthProvider::Keycloak,
            OAuthProvider::Google,
            OAuthProvider::AzureAd,
            OAuthProvider::Cognito,
            OAuthProvider::AwsIam,
            OAuthProvider::GitHub,
            OAuthProvider::GitLab,
            OAuthProvider::Facebook,
            OAuthProvider::X,
            OAuthProvider::Apple,
            OAuthProvider::Firebase,
            OAuthProvider::Okta,
            OAuthProvider::Auth0,
            OAuthProvider::Supabase,
            OAuthProvider::OneLogin,
            OAuthProvider::PingIdentity,
            OAuthProvider::Salesforce,
            OAuthProvider::Oracle,
            OAuthProvider::Ibm,
            OAuthProvider::JumpCloud,
            OAuthProvider::Duo,
            OAuthProvider::FusionAuth,
            OAuthProvider::Authentik,
            OAuthProvider::Zitadel,
            OAuthProvider::Casdoor,
            OAuthProvider::Logto,
            OAuthProvider::Clerk,
            OAuthProvider::Stytch,
            OAuthProvider::WorkOS,
            OAuthProvider::Descope,
        ];

        for provider in &providers {
            let prefix = provider.prefix();
            assert_eq!(prefix.len(), 3, "prefix for {:?} is not 3 chars", provider);
            let back = OAuthProvider::from_prefix(&prefix);
            assert_eq!(
                *provider, back,
                "prefix round-trip failed for {:?} (prefix={})",
                provider, prefix
            );
        }
    }

    #[test]
    fn test_prefix_no_duplicates() {
        let providers = [
            OAuthProvider::Keycloak,
            OAuthProvider::Google,
            OAuthProvider::AzureAd,
            OAuthProvider::Cognito,
            OAuthProvider::AwsIam,
            OAuthProvider::GitHub,
            OAuthProvider::GitLab,
            OAuthProvider::Facebook,
            OAuthProvider::X,
            OAuthProvider::Apple,
            OAuthProvider::Firebase,
            OAuthProvider::Okta,
            OAuthProvider::Auth0,
            OAuthProvider::Supabase,
            OAuthProvider::OneLogin,
            OAuthProvider::PingIdentity,
            OAuthProvider::Salesforce,
            OAuthProvider::Oracle,
            OAuthProvider::Ibm,
            OAuthProvider::JumpCloud,
            OAuthProvider::Duo,
            OAuthProvider::FusionAuth,
            OAuthProvider::Authentik,
            OAuthProvider::Zitadel,
            OAuthProvider::Casdoor,
            OAuthProvider::Logto,
            OAuthProvider::Clerk,
            OAuthProvider::Stytch,
            OAuthProvider::WorkOS,
            OAuthProvider::Descope,
        ];

        let mut prefixes: Vec<String> = providers.iter().map(|p| p.prefix()).collect();
        let original_len = prefixes.len();
        prefixes.sort();
        prefixes.dedup();
        assert_eq!(
            prefixes.len(),
            original_len,
            "duplicate prefixes found among well-known providers"
        );
    }

    #[test]
    fn test_prefix_custom_deterministic() {
        let p = OAuthProvider::Custom("https://my-idp.internal.corp".to_string());
        let prefix1 = p.prefix();
        let prefix2 = p.prefix();
        assert_eq!(prefix1, prefix2);
        assert_eq!(prefix1.len(), 3);
    }

    #[test]
    fn test_from_prefix_unknown_returns_custom() {
        let p = OAuthProvider::from_prefix("zzz");
        assert_eq!(p, OAuthProvider::Custom("zzz".to_string()));
    }
}
