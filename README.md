# fairOS-rs

Rust library for interaction with the FairOS API.

## Usage

- [User](#user)
- [Pod](#pod)
- [File System](#file-system)
- [Key Value Store](#key-value-store)
- [Document DB](#document-db)

### User

Sign up with mnemonic:

```rust
let mut fairos = Client::new();
let mut rng = ChaCha20Rng::from_entropy();
let mnemonic = Client::generate_mnemonic(&mut rng);
let (address, _) = fairos.signup("username", "password", Some(&mnemonic)).await.unwrap();
```

Sign up without mnemonic:

```rust
let mut fairos = Client::new();
let (address, mnemonic) = fairos.signup("username", "password", None).await.unwrap();
```

Log in:

```rust
fairos.login("username", "password").await.unwrap();
```

Import account with address:

```rust
let address = fairos.import_with_address("username", "password", "0x...").await.unwrap();
```

Import account with mnemonic:

```rust
let mnemonic = "brick salad slogan group happy exact wash way keen park amount concert";
let address = fairos.import_with_mnemonic("username", "password", mnemonic).await.unwrap();
```

Delete user:

```rust
fairos.delete_user("username", "password").await.unwrap();
```

Log out:

```rust
fairos.logout("username").await.unwrap();
```

Export user:

```rust
let export = fairos.export_user("username").await.unwrap();
println!("{:?}", export.username);
println!("{:?}", export.address);
```

### Pod

Create pod:

```rust
fairos.create_pod("username", "cat-photos", "password").await.unwrap();
```

Open pod:

```rust
fairos.open_pod("username", "cat-photos", "password").await.unwrap();
```

Sync pod:

```rust
fairos.sync_pod("username", "cat-photos").await.unwrap();
```

Close pod:

```rust
fairos.close_pod("username", "cat-photos").await.unwrap();
```

Share pod:

```rust
let reference = fairos.share_pod("username", "cat-photos", "password").await.unwrap();
```

Receive shared pod:

```rust
fairos.receive_shared_pod("second-user", &reference).await.unwrap();
```

Delete pod:

```rust
fairos.delete_pod("username", "cat-photos", "password").await.unwrap();
```

List pods:

```rust
let (pods, shared_pods) = fairos.list_pods("username").await.unwrap();
println!("{:?}", pods);
```

### File System

Make directory:

```rust
fairos.open_pod("username", "cat-photos", "password").await.unwrap();
fairos.mkdir("username", "cat-photos", "/Photos").await.unwrap();
```

Remove directory:

```rust
fairos.rmdir("username", "cat-photos", "/Photos").await.unwrap();
```

List directory:

```rust
let (dirs, files) = fairos.ls("username", "cat-photos", "/Photos").await.unwrap();
println!("{:?}", dirs);
println!("{:?}", files);
```

Upload file:

```rust
fairos
    .upload_file(
        "username",
        "cat-photos",
        "/Photos",
        "/home/user/Pictures/my-cute-cat.jpeg",
        BlockSize::from_megabytes(2),
        Some(Compression::Gzip),
    )
    .await
    .unwrap();
```

Upload buffer:

```rust
fairos
    .upload_buffer(
        "username",
        "cat-photos",
        "/",
        "cat-names.txt",
        "Peanut Butter, Cleo, Oreo, Smokey".as_bytes(),
        mime::TEXT_PLAIN,
        BlockSize::from_kilobytes(1),
        Some(Compression::Gzip),
    )
    .await
    .unwrap();
```

Download file:

```rust
fairos
    .download_file(
        "username",
        "cat-photos",
        "/Photos/my-cute-cat.jpeg",
        "/home/user/Downloads/cat-pic.jpeg"
    )
    .await
    .unwrap();
```

Download buffer:

```rust
let bytes = fairos
    .download_buffer(
        "username",
        "cat-photos",
        "/Photos/my-cute-cat.jpeg",
    )
    .await
    .unwrap();
```

Remove file:

```rust
let reference = fairos.rm("username", "cat-photos", "/Photos/my-cute-cat.jpeg").await.unwrap();
```

Share and receive file:

```rust
let reference = fairos.share_file("username", "cat-photos", "/Photos/my-cute-cat.jpeg").await.unwrap();
let file_path = fairos.receive_shared_file("second-user", "my-files", &reference, "/Documents/images").await.unwrap();
println!("{:?}", file_path); // "/Documents/images/my-cute-cat.jpeg"
```

### Key Value Store

### Document DB
