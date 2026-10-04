"""Own a Windows benchmark process tree without changing host-wide settings."""
import ctypes
from ctypes import wintypes
import subprocess


class BASIC_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [('process_time', ctypes.c_int64), ('job_time', ctypes.c_int64),
                ('flags', wintypes.DWORD), ('min_working_set', ctypes.c_size_t),
                ('max_working_set', ctypes.c_size_t), ('active_processes', wintypes.DWORD),
                ('affinity', ctypes.c_size_t), ('priority', wintypes.DWORD),
                ('scheduling_class', wintypes.DWORD)]


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in
                ('read_ops', 'write_ops', 'other_ops', 'read_bytes', 'write_bytes', 'other_bytes')]


class EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [('basic', BASIC_LIMIT_INFORMATION), ('io', IO_COUNTERS),
                ('process_memory', ctypes.c_size_t), ('job_memory', ctypes.c_size_t),
                ('peak_process_memory', ctypes.c_size_t), ('peak_job_memory', ctypes.c_size_t)]


class THREAD_ENTRY(ctypes.Structure):
    _fields_ = [('size', wintypes.DWORD), ('usage', wintypes.DWORD),
                ('thread_id', wintypes.DWORD), ('process_id', wintypes.DWORD),
                ('base_priority', wintypes.LONG), ('delta_priority', wintypes.LONG),
                ('flags', wintypes.DWORD)]


def kernel_api():
    """Declare pointer-sized handles explicitly for the Windows job/thread APIs."""
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    signatures = {
        'CreateJobObjectW': ([ctypes.c_void_p, wintypes.LPCWSTR], wintypes.HANDLE),
        'SetInformationJobObject': ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
        'AssignProcessToJobObject': ([wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
        'CloseHandle': ([wintypes.HANDLE], wintypes.BOOL),
        'CreateToolhelp32Snapshot': ([wintypes.DWORD, wintypes.DWORD], wintypes.HANDLE),
        'Thread32First': ([wintypes.HANDLE, ctypes.POINTER(THREAD_ENTRY)], wintypes.BOOL),
        'Thread32Next': ([wintypes.HANDLE, ctypes.POINTER(THREAD_ENTRY)], wintypes.BOOL),
        'OpenThread': ([wintypes.DWORD, wintypes.BOOL, wintypes.DWORD], wintypes.HANDLE),
        'ResumeThread': ([wintypes.HANDLE], wintypes.DWORD),
    }
    for name, (arguments, result) in signatures.items():
        getattr(api, name).argtypes = arguments
        getattr(api, name).restype = result
    return api


class WindowsProcessJob:
    """Kill only this run's proxy, workload and descendants when ownership ends."""

    def __init__(self):
        """Create a private, non-inheritable kill-on-close job; fail closed on errors."""
        self.api = kernel_api()
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = EXTENDED_LIMIT_INFORMATION()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def __enter__(self):
        """Retain the job handle throughout one workload run."""
        return self

    def __exit__(self, *_):
        """Close ownership even on timeout, interruption or abnormal proxy exit."""
        self.close()

    def close(self):
        """Close once; Windows then terminates any remaining owned descendants."""
        if self.handle:
            handle, self.handle = self.handle, None
            if not self.api.CloseHandle(handle):
                raise ctypes.WinError(ctypes.get_last_error())

    def start(self, command, **kwargs):
        """Assign a suspended proxy before it can spawn any workload descendants."""
        kwargs['creationflags'] = kwargs.get('creationflags', 0) | 0x00000004  # CREATE_SUSPENDED
        process = subprocess.Popen(command, **kwargs)
        try:
            if not self.api.AssignProcessToJobObject(self.handle, wintypes.HANDLE(int(process._handle))):
                raise ctypes.WinError(ctypes.get_last_error())
            self._resume_primary_thread(process.pid)
            return process
        except BaseException:
            process.terminate()
            process.wait(timeout=10)
            raise

    def _resume_primary_thread(self, process_id):
        """Resume only the newly created, still-suspended process's initial thread."""
        snapshot = self.api.CreateToolhelp32Snapshot(4, 0)  # TH32CS_SNAPTHREAD
        if snapshot == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            entry = THREAD_ENTRY()
            entry.size = ctypes.sizeof(entry)
            found = self.api.Thread32First(snapshot, ctypes.byref(entry))
            while found:
                if entry.process_id == process_id:
                    thread = self.api.OpenThread(0x0002, False, entry.thread_id)  # THREAD_SUSPEND_RESUME
                    if not thread:
                        raise ctypes.WinError(ctypes.get_last_error())
                    try:
                        if self.api.ResumeThread(thread) == 0xFFFFFFFF:
                            raise ctypes.WinError(ctypes.get_last_error())
                        return
                    finally:
                        self.api.CloseHandle(thread)
                found = self.api.Thread32Next(snapshot, ctypes.byref(entry))
            raise RuntimeError('Suspended benchmark proxy has no initial thread')
        finally:
            self.api.CloseHandle(snapshot)
