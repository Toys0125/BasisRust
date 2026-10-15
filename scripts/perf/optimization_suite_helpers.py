"""Small testable helpers for the Linux optimization suite runner."""

import json


def summarize_profile_workload(capture, output, module, failures):
    if not (capture / 'workload.json').exists():
        failures.append('profile-missing-workload')
        return
    try:
        result = module.summarize(capture)
    except Exception as error:
        failures.append('profile-validation')
        (output / 'avatar-1500-validation-error.txt').write_text(str(error) + '\n')
        print(json.dumps({'case': 'profile-avatar-1500', 'valid': False,
                          'error': str(error)}), flush=True)
        return
    (output / 'avatar-1500.summary.json').write_text(json.dumps(result, indent=2))
    if not result['valid']:
        failures.append('profile-validation')
    print(json.dumps({'case': 'profile-avatar-1500', 'valid': result['valid'],
                      'failed': [key for key, passed in result['checks'].items() if not passed],
                      'metrics': result['metrics']}), flush=True)
