ssh_options=-p 1722 -o StrictHostKeyChecking=no
remote=ubuntu@localhost
sshpass=sshpass -p ubuntu
ssh_with_sshpass=$(sshpass) ssh $(ssh_options) $(remote)

focal-server-cloudimg-arm64.img:
	wget https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-arm64.img

cloud.img: image/cloud.txt
	docker build -t tmp-cloud-image image
	docker create --name tmp-cloud-image tmp-cloud-image
	docker cp tmp-cloud-image:/cloud.img .
	docker rm tmp-cloud-image

QEMU_EFI.fd:
	wget https://releases.linaro.org/components/kernel/uefi-linaro/latest/release/qemu64/QEMU_EFI.fd

flash0.img: QEMU_EFI.fd
	dd if=/dev/zero of=flash0.img bs=1M count=64
	dd if=QEMU_EFI.fd of=flash0.img conv=notrunc

.PHONY: tkvs-server
tkvs-server:
	cd .. && nix develop .#aarch64-linux-cross -c cargo build --target aarch64-unknown-linux-gnu && cp target/aarch64-unknown-linux-gnu/debug/tkvs-server vm-test/tkvs-server

# vmを作り直すときはdisk.imgを削除する
disk.img: focal-server-cloudimg-arm64.img cloud.img flash0.img
	dd if=/dev/zero of=flash1.img bs=1M count=64
	cp focal-server-cloudimg-arm64.img disk.img
	ssh-keygen -f ~/.ssh/known_hosts -R "[localhost]:1722"


.PHONY: up-vm
up-vm:
	qemu-system-aarch64 -m 1024 -cpu cortex-a57 -M virt -nographic \
		-drive if=pflash,format=raw,readonly=true,file=flash0.img \
		-drive if=pflash,format=raw,file=flash1.img \
		-drive if=none,file=disk.img,id=hd0 \
		-device virtio-blk-device,drive=hd0 \
		-drive if=none,readonly=true,id=cloud,file=cloud.img \
		-device virtio-blk-device,drive=cloud \
		-device virtio-net-device,netdev=net \
		-netdev user,id=net,hostfwd=tcp::1722-:22,hostfwd=tcp::17051-:50051 \

.PHONY: wait-vm
wait-vm:
	while true; do sleep 10; if $(ssh_with_sshpass) echo; then break; fi; done

.PHONY: copy-tkvs-server
copy-tkvs-server: tkvs-server
	$(ssh_with_sshpass) mkdir -p /home/ubuntu/tkvs
	$(sshpass) rsync -ave "ssh $(ssh_options)" tkvs-server $(remote):/home/ubuntu/tkvs/tkvs-server

.PHONY: run-tkvs-server
run-tkvs-server:
	$(ssh_with_sshpass) mkdir -p /home/ubuntu/tkvs/data
	$(ssh_with_sshpass) TKVS_DATA=/home/ubuntu/tkvs/data TKVS_IP=0.0.0.0 /home/ubuntu/tkvs/tkvs-server
