#!/usr/bin/env python3
"""Export resolved CPU sample leaves, including CLR methods, with WPA.

Use a completed --include-clr native capture. xperf's native stack export does
not resolve the JIT address ranges; WPA consumes the CLR rundown metadata.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('capture', type=Path)
    parser.add_argument('--pid', required=True, type=int)
    parser.add_argument('--process-name', default='BasisNetworkConsole.exe')
    args = parser.parse_args()
    if args.pid <= 0 or '"' in args.process_name:
        parser.error('a positive PID and an unquoted process name are required')
    capture = args.capture.resolve()
    state = json.loads((capture / 'native-status.json').read_text(encoding='utf-8-sig'))
    if state.get('phase') != 'complete' or not state.get('includeClr'):
        parser.error('a completed CLR-enabled capture is required')
    exporter = shutil.which('wpaexporter')
    rtk = shutil.which('rtk')
    if not exporter or not rtk:
        parser.error('wpaexporter and rtk are required on PATH')
    kit = Path(exporter).parent
    ns = {'w': 'http://tempuri.org/SerializableElement.xsd'}
    ET.register_namespace('', ns['w'])
    ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    tree = ET.parse(kit / 'Catalog/DotNETRuntime.wpaprofile')
    tree.find('.//w:FileReferences', ns).clear()
    graphs = tree.find('.//w:Graphs', ns)
    for graph in list(graphs)[1:]:
        graphs.remove(graph)
    preset = graphs[0].find('w:Preset', ns)
    preset.set('InitialSelectionQuery', '')
    preset.set('InitialFilterQuery', f'[Process]:="{args.process_name} ({args.pid})"')
    preset.set('InitialFilterShouldKeep', 'true')
    preset.set('KeyColumnCount', '1')
    columns = preset.find('w:Columns', ns)
    function = next(c for c in columns if c.get('Name') == 'Function')
    columns.remove(function)
    columns.insert(0, function)
    for column in columns:
        column.set('IsVisible', str(column.get('Name') in
            ('Function', 'Count', 'Weight (in view)', '% Weight')).lower())
    analysis = capture / 'analysis/wpa'
    analysis.mkdir(parents=True, exist_ok=True)
    profile = analysis / 'cpu-leaves.wpaprofile'
    tree.write(profile, encoding='utf-8', xml_declaration=True)
    env = dict(os.environ)
    env['_NT_SYMBOL_PATH'] = (str(capture / 'symbols') + ';srv*' +
        str(capture / 'microsoft-symbols') + '*https://msdl.microsoft.com/download/symbols')
    env['_NT_SYMCACHE_PATH'] = str(capture / 'symcache')
    log_path = analysis / 'export.log'
    csv_path = analysis / 'CPU_Usage_(Sampled)_Utilization_By_Process.csv'
    # Do not mistake a previous export for success when WPA returns zero after
    # a preset/filter error. This file is owned by this specific export.
    csv_path.unlink(missing_ok=True)
    with log_path.open('w') as log:
        result = subprocess.run([rtk, 'proxy', exporter, '-i', str(capture / 'cpu-stacks.etl'),
            '-symbols', '-cliprundown', '-profile', str(profile), '-outputfolder', str(analysis)],
            env=env, stdout=log, stderr=subprocess.STDOUT)
    log_text = log_path.read_text(errors='replace')
    if result.returncode or 'Error exporting profile' in log_text:
        raise RuntimeError(f'WPA export failed; see {log_path}')
    totals = {}
    with csv_path.open(encoding='utf-8-sig', newline='') as source:
        for row in csv.DictReader(source):
            leaf = totals.setdefault(row['Function'], {'samples': 0, 'weight_ms': 0.0})
            leaf['samples'] += int(row['Count'].replace(',', ''))
            leaf['weight_ms'] += float(row['Weight (in view) (ms)'].replace(',', ''))
    count = sum(v['samples'] for v in totals.values())
    if not count:
        raise RuntimeError('No samples exported for the requested process')
    leaves = sorted([{'function': name, **value, 'exclusive_percent': value['samples'] / count * 100}
        for name, value in totals.items()], key=lambda row: row['samples'], reverse=True)
    output = analysis / 'managed-leaves.json'
    output.write_text(json.dumps({'pid': args.pid, 'process_name': args.process_name,
        'sample_count': count, 'attribution': 'exclusive CPU sample leaves; CLR rundown clipped',
        'leaves': leaves}, indent=2) + '\n')
    print(f'Exported {count} CPU samples to {output}')


if __name__ == '__main__':
    main()
