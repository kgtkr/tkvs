export
ssh_port=1722
ssh_options=-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
remote=arch@localhost
sshpass=sshpass -p arch
ssh_with_sshpass=$(sshpass) ssh $(ssh_options) -p $(ssh_port) $(remote)
grpc_port=17051

.PHONY: run
run:
	cargo run

.PHONY: runN
runN:
	set -e && for i in `seq 1 10`; do echo ===========loop$$i; make run; done

.PHONY: tkvs-server
tkvs-server:
	cd .. && nix develop .#x86_64-linux-cross -c cargo build --target x86_64-unknown-linux-musl && cp target/x86_64-unknown-linux-musl/debug/tkvs-server crash-test/tkvs-server

image.qcow2:
	wget $$(cat image-url) -O image.qcow2

disk.qcow2: image.qcow2
	cp image.qcow2 disk.qcow2

.PHONY: clean-vm
clean-vm:
	rm disk.qcow2

.PHONY: up-vm
up-vm: disk.qcow2
	qemu-system-x86_64 -m 1024 -smp 10 -nographic \
		-drive file=disk.qcow2,if=virtio \
		-net nic -net user,hostfwd=tcp::$(ssh_port)-:22,hostfwd=tcp::$(grpc_port)-:50051

.PHONY: wait-vm
wait-vm:
	while true; do sleep 2; if $(ssh_with_sshpass) echo; then break; fi; done

.PHONY: reset-tkvs-server
reset-tkvs-server: tkvs-server
	$(ssh_with_sshpass) "rm -rf /home/arch/tkvs"
	$(ssh_with_sshpass) "mkdir -p /home/arch/tkvs/data"
	$(sshpass) scp $(ssh_options) -P $(ssh_port) tkvs-server $(remote):/home/arch/tkvs/tkvs-server

.PHONY: run-tkvs-server
run-tkvs-server:
	$(ssh_with_sshpass) "TKVS_DATA=/home/arch/tkvs/data TKVS_IP=0.0.0.0 /home/arch/tkvs/tkvs-server"
