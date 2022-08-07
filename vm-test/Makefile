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

build:
	cd .. && cross build --target aarch64-unknown-linux-gnu
