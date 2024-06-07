Import('env', 'envCython', 'arch', 'common')


visionipc_dir = Dir('visionipc')
gen_dir = Dir('gen')


# Build messaging


messaging_objects = env.SharedObject([
  'msgq/ipc.cc',
  'msgq/event.cc',
  'msgq/impl_zmq.cc',
  'msgq/impl_msgq.cc',
  'msgq/impl_fake.cc',
  'msgq/msgq.cc',
])
messaging = env.Library('messaging', messaging_objects)
messaging_python = envCython.Program('msgq/messaging_pyx.so', 'msgq/messaging_pyx.pyx', LIBS=envCython["LIBS"]+[messaging, "zmq", common])


# Build Vision IPC
vipc_files = ['ipc.cc', 'visionipc_server.cc', 'visionipc_client.cc', 'visionbuf.cc']
vipc_sources = [f'{visionipc_dir.abspath}/{f}' for f in vipc_files]

if arch == "larch64":
  vipc_sources += [f'{visionipc_dir.abspath}/visionbuf_ion.cc']
else:
  vipc_sources += [f'{visionipc_dir.abspath}/visionbuf_cl.cc']

vipc_objects = env.SharedObject(vipc_sources)
visionipc = env.Library('visionipc', vipc_objects)


vipc_frameworks = []
vipc_libs = envCython["LIBS"] + [visionipc, messaging, common, "zmq"]
if arch == "Darwin":
  vipc_frameworks.append('OpenCL')
else:
  vipc_libs.append('OpenCL')
envCython.Program(f'{visionipc_dir.abspath}/visionipc_pyx.so', f'{visionipc_dir.abspath}/visionipc_pyx.pyx',
                  LIBS=vipc_libs, FRAMEWORKS=vipc_frameworks)

if GetOption('extras'):
  env.Program('msgq/test_runner', ['msgq/test_runner.cc', 'msgq/msgq_tests.cc'], LIBS=[messaging, common])
  env.Program('visionipc/test_runner',
             ['visionipc/test_runner.cc', 'visionipc/visionipc_tests.cc'],
              LIBS=['pthread'] + vipc_libs, FRAMEWORKS=vipc_frameworks)

Export('visionipc', 'messaging', 'messaging_python')
