# Micro-VM runtime assets + counter demo (kind, fetch-not-bake)

The `microvm` runtime (`cmd/ateworker-microvm`, kata + cloud-hypervisor) fetches its
toolchain at runtime — nothing kata-specific is baked into the worker image. ateworker drives
the kata-agent directly (no kata shim, no containerd). The guest mounts each container's
rootfs as an overlay of the image's EROFS layers (read-only `virtio-pmem` devices with DAX)
and a writable upper on the host, served by `virtiofsd`. The asset set is four files:

- `cloud-hypervisor` — the VMM binary (fetched from its release)
- `virtiofsd` — the virtio-fs daemon serving the writable upper and volumes (from kata-static)
- `vmlinux` — the guest kernel (from kata-static)
- `rootfs.img` — the guest rootfs image (from kata-static)

The guest kernel needs `erofs`, `virtio_pmem`, and `FS_DAX`; Kata's default guest kernel
config enables them. An actor whose containers use more than 155 distinct layers is refused.

An optional `mke2fs` asset (e2fsprogs 1.47 or later, statically linked, since the
worker image has no libc) moves those containers' writable upper off the virtio-fs
share onto a per-container virtio-blk ext4 disk, so a checkpoint ships one image
file instead of archiving the upper file by file. `assemble.sh` does not produce
it; to build one:

```sh
curl -fsSLO https://mirrors.edge.kernel.org/pub/linux/kernel/people/tytso/e2fsprogs/v1.47.1/e2fsprogs-1.47.1.tar.gz
tar xzf e2fsprogs-1.47.1.tar.gz && cd e2fsprogs-1.47.1
./configure CFLAGS="-O2 -std=gnu17" LDFLAGS=-static --disable-nls --disable-fuse2fs \
  --disable-elf-shlibs --enable-libuuid --enable-libblkid
make libs && make -C misc mke2fs && strip -o mke2fs misc/mke2fs
```

Then add it to the `SandboxConfig` version's assets next to `virtiofsd` (key `mke2fs`,
with its `url` and `sha256`). Actors keep the upper layout their snapshot recorded.

These helpers assemble the asset set for your node arch, stage it into the cluster's rustfs
S3 bucket, and the demo manifest's `SandboxConfig` points at it. When `/dev/kvm` is
available, `hack/create-kind-cluster.sh` mounts it into the node; atelet then advertises
it as a device, which is what places micro-VM workers there.

> [!TIP]
> `hack/run-microvm-demo.sh` automates the full bring-up below (assets, control plane,
> demo apply) for kind OR GKE without editing committed files. The steps here are the
> manual equivalent.

## Steps (run on a KVM-capable Linux host matching the node arch)

1. **Assemble assets for your arch:**
   ```sh
   ARCH=arm64 hack/microvm-assets/assemble.sh
   ```

2. **Bring up the cluster + control plane:**
   ```sh
   hack/create-kind-cluster.sh        # mounts /dev/kvm into the nodes
   hack/install-ate-kind.sh           # control plane + rustfs (bucket: ate-snapshots)
   ```

3. **Stage assets into rustfs:**
   ```sh
   OUT="$PWD/microvm-assets-arm64" hack/microvm-assets/stage-to-rustfs.sh
   ```

4. **Apply the demo + drive it:**
   ```sh
   BUCKET_NAME=ate-snapshots envsubst < demos/counter/counter-microvm.yaml.tmpl | kubectl apply -f -
   ```
   Create an actor from `counter-microvm`, hit the in-RAM counter to increment it, suspend
   (checkpoint), resume on a different worker pod, and confirm the count continues — proving the
   guest-memory snapshot round-tripped across pods.

## Notes
- Assets must not be stored under a snapshot location (an ActorTemplate's
  `snapshotConfig.location` followed by `/atespaces/`): the node's object-store plugin
  refuses to serve anything there as an asset. The staging scripts use `kata-assets/`,
  which is outside it. On GKE they stage into the asset bucket, `BUCKET_NAME-assets`,
  the only bucket setup-gcp lets nodes read. On kind they share the rustfs bucket.
- `assets` is single-arch (unlike runsc's amd64/arm64): stage assets matching the node arch.
