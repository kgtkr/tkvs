ssh_options="-p 7122 -o StrictHostKeyChecking=no -i vm_rsa"
ssh_command="ssh $ssh_options ubuntu@localhost"

run-vm() {
    make focal-server-cloudimg-arm64.img cloud.img flash0.img
    dd if=/dev/zero of=flash1.img bs=1M count=64
    cp focal-server-cloudimg-arm64.img disk.img
    qemu-system-aarch64 -m 1024 -cpu cortex-a57 -M virt -nographic \
        -drive if=pflash,format=raw,readonly=true,file=flash0.img \
        -drive if=pflash,format=raw,file=flash1.img \
        -drive if=none,file=disk.img,id=hd0 \
        -device virtio-blk-device,drive=hd0 \
        -drive if=none,readonly=true,id=cloud,file=cloud.img \
        -device virtio-blk-device,drive=cloud \
        -device virtio-net-device,netdev=net \
        -netdev user,id=net,hostfwd=tcp::7122-:22 > /dev/null 2>&1 &
    while true; do
        sleep 10
        if $ssh_command echo; then
            break
        fi
    done
}
