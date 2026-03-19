
            /// Returns the `rustc` SemVer version and additional metadata
            /// like the git short hash and build date.
            pub fn version_meta() -> VersionMeta {
                VersionMeta {
                    semver: Version {
                        major: 1,
                        minor: 93,
                        patch: 1,
                        pre: Prerelease::new("").unwrap(),
                        build: BuildMetadata::new("").unwrap(),
                    },
                    host: "aarch64-unknown-linux-gnu".to_owned(),
                    short_version_string: "rustc 1.93.1 (01f6ddf75 2026-02-11)".to_owned(),
                    commit_hash: Some("01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf".to_owned()),
                    commit_date: Some("2026-02-11".to_owned()),
                    build_date: None,
                    channel: Channel::Stable,
                    llvm_version: Some(LlvmVersion{ major: 21, minor: 1 }),
                }
            }
            