"""Shared strict PGO validation: original quality gates plus native admission telemetry."""
import json, math
from avatar_tuning import summarize as original_summarize, interpolate_health_counter

COUNTERS = ('admittedRecipientMessages', 'rejectedRecipientMessages', 'enqueuedMessages',
            'dequeuedMessages', 'attemptedRecipientMessages', 'staleRecipientMessages',
            'sendErrors', 'enqueueErrors', 'queueBackpressure', 'fanoutBackpressure', 'sentDatagrams')

def admission_metrics(samples, start, end, limit):
    seconds = end - start
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('Invalid fixed measurement window')
    rows = [sample['health']['extended']['sceneBatch'] for sample in samples]
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError('Missing native Scene admission telemetry')
    for row in rows:
        if row['peerQueueLimit'] != limit:
            raise ValueError('Scene queue limit differs from selected arm')
        for key in (*COUNTERS, 'pendingRecipientMessages', 'peakPeerPendingMessages'):
            if isinstance(row[key], bool) or not isinstance(row[key], int) or row[key] < 0:
                raise ValueError('Invalid Scene counter: ' + key)
    for first, last in zip(rows, rows[1:]):
        if any(last[key] < first[key] for key in COUNTERS):
            raise ValueError('Scene admission counter reset')
    deltas = {key: interpolate_health_counter(samples, end, 'sceneBatch', key)
                   - interpolate_health_counter(samples, start, 'sceneBatch', key)
              for key in COUNTERS}
    metrics = {
        'scene_peer_rejected_recipient_messages_per_second': deltas['rejectedRecipientMessages'] / seconds,
        'scene_peer_admitted_recipient_messages_per_second': deltas['admittedRecipientMessages'] / seconds,
        'scene_peer_peak_pending_messages': max(row['peakPeerPendingMessages'] for row in rows),
        'scene_peer_pending_recipient_messages_max': max(row['pendingRecipientMessages'] for row in rows),
    }
    offered = deltas['admittedRecipientMessages'] + deltas['rejectedRecipientMessages']
    metrics['scene_peer_admission_ratio'] = deltas['admittedRecipientMessages'] / offered if offered else 0
    checks = {
        'scene_admission_telemetry': True,
        'zero_scene_peer_rejections': all(row['rejectedRecipientMessages'] == 0 for row in rows),
        'scene_peer_queue_bound': limit == 0 or metrics['scene_peer_peak_pending_messages'] <= limit,
        'zero_scene_relay_errors': all(row['enqueueErrors'] == row['sendErrors'] == 0 for row in rows),
        'zero_scene_stale_recipients': all(row['staleRecipientMessages'] == 0 for row in rows),
    }
    return metrics, checks, deltas

def scene_summarize(path, experiment, entry):
    result = original_summarize(path, experiment, entry)
    result['original_checks'] = dict(result['checks'])
    result['original_valid'] = result['valid']
    samples = [json.loads(line) for line in (path / 'samples.jsonl').read_text().splitlines()]
    meta = json.loads((path / 'run.json').read_text())
    limit = int(entry['server_settings']['BASIS_SCENE_PEER_QUEUE_LIMIT'])
    metrics, checks, deltas = admission_metrics(samples, meta['window_start_monotonic'],
                                               meta['window_end_monotonic'], limit)
    result['metrics'].update(metrics)
    result['checks'].update(checks)
    result['scene_admission_window_deltas'] = deltas
    result['valid'] = all(result['checks'].values())
    return result

UPLOAD_COUNTERS = ('acceptedMessages', 'acceptedBytes', 'rejectedMessages', 'rejectedBytes',
            'reliableRejections', 'oversizedReliableMessages', 'imageExemptMessages', 'imageExemptBytes')

def upload_metrics(samples, start, end, limit, clients):
    seconds = end-start
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('Invalid fixed measurement window')
    rows = [sample['health']['extended']['uploadBudget'] for sample in samples]
    if len(rows) < 2:
        raise ValueError('Insufficient native upload telemetry')
    for row in rows:
        if row['bytesPerSecondPerPlayer'] != limit or row['burstSeconds'] != 2:
            raise ValueError('Upload budget differs from selected arm')
        for key in UPLOAD_COUNTERS:
            if isinstance(row[key], bool) or not isinstance(row[key], int) or row[key] < 0:
                raise ValueError('Invalid upload counter: '+key)
    for first,last in zip(rows, rows[1:]):
        if any(last[key]<first[key] for key in UPLOAD_COUNTERS):
            raise ValueError('Upload counter reset')
    deltas = {key: interpolate_health_counter(samples,end,'uploadBudget',key)
                  - interpolate_health_counter(samples,start,'uploadBudget',key) for key in UPLOAD_COUNTERS}
    metrics = {'upload_'+key+'_per_second':value/seconds for key,value in deltas.items()}
    metrics['upload_accepted_bytes_per_client_per_second'] = deltas['acceptedBytes']/seconds/clients
    # Readable aliases in reports/controllers; native field names remain in deltas.
    metrics['upload_rejected_messages_per_second'] = deltas['rejectedMessages']/seconds
    checks = {'upload_telemetry':True,
              'positive_upload_input':deltas['acceptedBytes']>0 and deltas['acceptedMessages']>0,
              'zero_upload_rejections':all(row['rejectedMessages']==row['rejectedBytes']==row['reliableRejections']==0 for row in rows),
              'no_oversized_reliable_workload':all(row['oversizedReliableMessages']==0 for row in rows),
              'no_image_workload':all(row['imageExemptMessages']==row['imageExemptBytes']==0 for row in rows)}
    return metrics,checks,deltas

def upload_summarize(path, experiment, entry):
    result = scene_summarize(path,experiment,entry)
    samples = [json.loads(line) for line in (path/'samples.jsonl').read_text().splitlines()]
    meta = json.loads((path/'run.json').read_text())
    limit = int(entry['server_settings']['MaxUploadBytesPerSecondPerPlayer'])
    metrics,checks,deltas = upload_metrics(samples,meta['window_start_monotonic'],meta['window_end_monotonic'],limit,experiment['workload']['clients'])
    result['metrics'].update(metrics)
    result['checks'].update(checks)
    result['upload_window_deltas'] = deltas
    result['valid'] = all(result['checks'].values())
    return result

def summarize_pgo(path,experiment,entry):
    chosen={**experiment,'frozen':{**experiment['frozen'],'server':experiment['frozen'][entry['variant']]}}
    result=upload_summarize(path,chosen,entry)
    samples=[json.loads(line) for line in (path/'samples.jsonl').read_text().splitlines()]
    rows=[sample['health']['extended']['images'] for sample in samples]
    fields=['egressDroppedMessages','egressDroppedBytes','cachedImages','completeImages','cacheBytes']
    for row in rows:
        if any(isinstance(row[key],bool) or not isinstance(row[key],int) or row[key]<0 for key in fields):
            raise ValueError('Invalid image telemetry')
    result['image_observations']={'available':True,'first':rows[0],'last':rows[-1],'max':{key:max(row[key] for row in rows) for key in fields}}
    result['checks']['no_image_drops_or_cache_work']=all(row[key]==0 for row in rows for key in fields)
    result['valid']=all(result['checks'].values())
    return result
