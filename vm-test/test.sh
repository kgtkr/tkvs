ssh_options="-p 7122 -o StrictHostKeyChecking=no"
ssh_command="sshpass -p ubuntu ssh $ssh_options ubuntu@localhost"

run-vm() {
    make disk.img
    make up-vm > /dev/null 2>&1 &
    while true; do
        sleep 10
        if $ssh_command echo; then
            break
        fi
    done
}
