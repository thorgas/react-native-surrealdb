"""Explicit ownership of only the reverse mappings created by this runner."""
PLANNED_PORTS = (8080, 19061, 19062)


def snapshot(device):
    reverse = device('reverse', '--list')
    reverse.check_returncode()
    for line in reverse.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[-2] in {f'tcp:{p}' for p in PLANNED_PORTS}:
            raise RuntimeError('Planned reverse port already mapped; refusing to alter it')
    setting = device('shell', 'settings', 'get', 'global', 'hide_error_dialogs')
    setting.check_returncode()
    return {'reverse': reverse.stdout, 'hideErrorDialogs': setting.stdout.strip()}


def create_mappings(device, created):
    for port in PLANNED_PORTS:
        device('reverse', f'tcp:{port}', f'tcp:{port}').check_returncode()
        created.append(port)


def restore(device, baseline, created):
    errors = []
    mappings = None
    try:
        current = device('reverse', '--list')
        current.check_returncode()
        mappings = {tuple(line.split()[-2:]) for line in current.stdout.splitlines() if len(line.split()) >= 3}
    except Exception as error:
        errors.append(f'reverse readback failed: {error}')
    for port in reversed(created):
        if mappings is None:
            continue
        local = f'tcp:{port}'
        if (local, local) not in mappings:
            if any(pair[0] == local for pair in mappings):
                errors.append(f'tcp:{port} changed owner; preserved')
            continue
        try:
            device('reverse', '--remove', local).check_returncode()
        except Exception as error:
            errors.append(f'remove tcp:{port} failed: {error}')
    prior = baseline['hideErrorDialogs']
    args = ('delete', 'global', 'hide_error_dialogs') if prior == 'null' else ('put', 'global', 'hide_error_dialogs', prior)
    try:
        device('shell', 'settings', *args).check_returncode()
    except Exception as error:
        errors.append(f'restore setting failed: {error}')
    return errors
