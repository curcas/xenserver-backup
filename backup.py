from ftplib import FTP
from XenAPI import Session
import requests
from os import path, makedirs, remove
import time

###############################################
################ configuration ################
###############################################

# FTP
ftp_host = "localhost"
ftp_user = "anonymous"
ftp_pw = "anonymous"
ftp_dir = "/backup"
ftp_count = 10

#xenserver
xen_host = "https://localhost/"
xen_user = "root"
xen_pw = "root"

#arbitrary
arb_backup_path = "/backup"
arb_exclude = []
arb_vms_without_ram = []
arb_time = time.strftime("%Y-%m-%d-%H-%M-%S")


def get_session():
    session = Session(xen_host)
    session.login_with_password(xen_user, xen_pw)

    return session


def get_all_vms(session):
    return session.xenapi.VM.get_all()


def snapshot_vm(session, record, vm):
    if record["name_label"] not in arb_vms_without_ram and record["power_state"] != "Halted":
        return session.xenapi.VM.checkpoint(vm, record["name_label"] + "_" + arb_time)
    else:
        return session.xenapi.VM.snapshot(vm, record["name_label"] + "_" + arb_time)


def export_vm(snapshot_id, vm_name):
    if not path.exists(path.join(arb_backup_path, vm_name)):
        makedirs(path.join(arb_backup_path, vm_name))

    url = xen_host + "export?uuid=" + snapshot_id

    r = requests.get(url, stream=True, auth=(xen_user, xen_pw), verify=False)
    if r.status_code == 200:
        with open(path.join(arb_backup_path, vm_name, vm_name + "_" + arb_time + ".xva"), "wb") as f:
            for chunk in r.iter_content(1024 * 1024 * 10):
                f.write(chunk)


def upload_vm(vm_name):
    f = FTP(ftp_host)
    f.login(ftp_user, ftp_pw)
    f.cwd(ftp_dir)

    if vm_name not in f.nlst():
        f.mkd(vm_name)

    f.cwd(vm_name)

    filename = vm_name + "_" + arb_time + ".xva"

    f.storbinary("STOR " + filename, open(path.join(arb_backup_path, vm_name, filename), "rb"))
    f.close()


def cleanup_backup(vm_name):
    f = FTP(ftp_host)
    f.login(ftp_user, ftp_pw)
    f.cwd(ftp_dir)

    f.cwd(vm_name)

    files = f.nlst()

    if len(files) > ftp_count:
        files.sort()
        for file_name in files[:-ftp_count]:
            f.delete(file_name)


def delete_snapshot(session, vm, snapshot_id):
    record = session.xenapi.VM.get_record(vm)

    for snapshot in record["snapshots"]:
        uuid = session.xenapi.VM.get_uuid(snapshot)
        if uuid == snapshot_id:
            session.xenapi.VM.destroy(snapshot)


def delete_export(vm_name):
    remove(path.join(arb_backup_path, vm_name, vm_name + "_" + arb_time + ".xva"))


def backup():
    session = get_session()
    vms = get_all_vms(session)

    for vm in vms:
        record = session.xenapi.VM.get_record(vm)
        if record["name_label"] not in arb_exclude and record["is_a_template"] is False and record["is_control_domain"] is False:
            snapshot = snapshot_vm(session, record, vm)
            snapshot_id = session.xenapi.VM.get_uuid(snapshot)
            export_vm(snapshot_id, record["name_label"])
            upload_vm(record["name_label"])
            cleanup_backup(record["name_label"])
            delete_snapshot(session, vm, snapshot_id)
            delete_export(record["name_label"])


backup()
