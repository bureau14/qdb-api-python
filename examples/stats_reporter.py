import argparse
import logging
import sys
import time
import re

import quasardb
import quasardb.stats as qdbst


logger = logging.getLogger('create_tables')
#logging.basicConfig(level=logging.WARN)
#logger.setLevel(logging.INFO)


modules = [{'id': 'requests',
            'help': 'Request stats'},
           {'id': 'persistence',
            'help': 'Storage layer stats'},
           {'id': 'async',
            'help': 'Cumulative async pipeline stats'},
           {'id': 'async_details',
            'help': 'Detailed stats per async pipeline',
            'arg': '--async-details'},

           ]
def get_args():
    parser = argparse.ArgumentParser(
        description="Report statistics")

    parser.add_argument(
        '--cluster',
        dest='cluster_uri',
        help='QuasarDB cluster uri to connect to. Defaults to qdb://127.0.0.1:2836',
        default="qdb://127.0.0.1:2836")

    parser.add_argument(
        '--cluster-public-key',
        dest='cluster_public_key',
        help='Cluster public key file')

    parser.add_argument(
        '--user-security-file',
        dest='user_security_file',
        help='User security file, containing both username and private access token.')

    parser.add_argument(
        '--interval',
        dest='interval',
        type=int,
        default=1,
        help='Refresh interval in seconds.')

    parser.add_argument(
        '--node-id',
        dest='node_id',
        default='127.0.0.1:2836',
        help='Node ID to log stats for')

    module_group = parser.add_argument_group('Modules to enable/disable')

    for module in modules:
        arg = module['arg'] if 'arg' in module else '--{}'.format(module['id'])
        module_group.add_argument(arg,
                                  action='store_true',
                                  dest=module['id'],
                                  help=module['help'])


    return parser.parse_args()



###################
# ANSII color rendering
###################

color = {
    'black': '\033[0;30m',
    'darkred': '\033[0;31m',
    'darkgreen': '\033[0;32m',
    'darkyellow': '\033[0;33m',
    'darkblue': '\033[0;34m',
    'darkmagenta': '\033[0;35m',
    'darkcyan': '\033[0;36m',
    'gray': '\033[0;37m',

    'darkgray': '\033[1;30m',
    'red': '\033[1;31m',
    'green': '\033[1;32m',
    'yellow': '\033[1;33m',
    'blue': '\033[1;34m',
    'magenta': '\033[1;35m',
    'cyan': '\033[1;36m',
    'white': '\033[1;37m',

    'blackbg': '\033[40m',
    'redbg': '\033[41m',
    'greenbg': '\033[42m',
    'yellowbg': '\033[43m',
    'bluebg': '\033[44m',
    'magentabg': '\033[45m',
    'cyanbg': '\033[46m',
    'whitebg': '\033[47m',
}

ansi = {
    'reset': '\033[0;0m',
    'bold': '\033[1m',
    'reverse': '\033[2m',
    'underline': '\033[4m',

    'clear': '\033[2J',
#   'clearline': '\033[K',
    'clearline': '\033[2K',
    'save': '\033[s',
    'restore': '\033[u',
    'save_all': '\0337',
    'restore_all': '\0338',
    'linewrap': '\033[7h',
    'nolinewrap': '\033[7l',

    'up': '\033[1A',
    'down': '\033[1B',
    'right': '\033[1C',
    'left': '\033[1D',

    'default': '\033[0;0m',
}

char = {
    'pipe': '|',
    'colon': ':',
    'gt': '>',
    'space': ' ',
    'dash': '-',
    'plus': '+',
    'underscore': '_',
    'sep': ',',
}

theme = {
            'title': color['darkblue'],
            'subtitle': color['blue'] + ansi['underline'],
            'frame': color['darkblue'],
            'default': ansi['default'],
            'error': color['white'] + color['redbg'],
            'roundtrip': color['darkblue'],
            'debug': color['darkred'],
            'input': color['darkgray'],
            'done_lo': color['white'],
            'done_hi': color['gray'],
            'text_lo': color['gray'],
            'text_hi': color['darkgray'],
            'unit_lo': color['darkgray'],
            'unit_hi': color['darkgray'],
            'colors_lo': (color['red'], color['yellow'], color['green'], color['blue'],
                          color['cyan'], color['white'], color['darkred'], color['darkgreen']),
            'colors_hi': (color['darkred'], color['darkyellow'], color['darkgreen'], color['darkblue'],
                          color['darkcyan'], color['gray'], color['red'], color['green'])}
step = 1

def fchg(var, width, base):
    "Convert float to string given scale and length"
    c = 0
    while True:
        if var == 0:
            ret = str('0')
            break
#       ret = repr(round(var))
#       ret = repr(int(round(var, maxlen)))
        ret = str(int(round(var, width)))
        if len(ret) <= width:
            i = width - len(ret) - 1
            while i > 0:
                ret = ('%%.%df' % i) % var
                if len(ret) <= width and ret != str(int(round(var, width))):
                    break
                i = i - 1
            else:
                ret = str(int(round(var)))
            break
        var = var / base
        c = c + 1
    else:
        c = -1
    return ret, c

def cprint(var, ctype = 'f', width = 4, scale = 1000):
    "Color print one column"

    base = 1000
    if scale == 1024:
        base = 1024

    ### Use units when base is exact 1000 or 1024
    unit = False
    if scale in (1000, 1024) and width >= len(str(base)):
        unit = True
        width = width - 1

    ### If this is a negative value, return a dash
    if ctype != 's' and var < 0:
        if unit:
            return theme['error'] + '-'.rjust(width) + char['space'] + theme['default']
        else:
            return theme['error'] + '-'.rjust(width) + theme['default']

    units = ('B', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')

    colors = theme['colors_hi']
    ctext = theme['text_hi']
    cunit = theme['unit_hi']
    cdone = theme['done_hi']

    ### Convert value to string given base and field-length
    if ctype in ('b', 'd', 'p'):
        ret, c = dchg(var, width, base)
    elif ctype in ('f',):
        ret, c = fchg(var, width, base)
    elif ctype in ('s',):
        ret, c = str(var), ctext
    elif ctype in ('t',):
        ret, c = tchg(var, width), ctext
    else:
        raise Exception('Type %s not known to dstat.' % ctype)

    ### Set the counter color
    if ret == '0':
        color = cunit
    elif scale <= 0:
        color = ctext
    elif ctype in ('p') and round(var) >= 100.0:
        color = cdone
#    elif type in ('p'):
#        color = colors[int(round(var)/scale)%len(colors)]
    elif scale not in (1000, 1024):
        color = colors[int(var/scale)%len(colors)]
    elif ctype in ('b', 'd', 'f'):
        color = colors[c%len(colors)]
    else:
        color = ctext

    ### Justify value to left if string
    if ctype in ('s',):
        ret = color + ret.ljust(width)
    else:
        ret = color + ret.rjust(width)

    ### Add unit to output
    if unit:
        if c != -1 and round(var) != 0:
            ret += cunit + units[c]
        else:
            ret += char['space']

    return ret

def cprintlist(varlist, ctype, width, scale):
    "Return all columns color printed"
    ret = sep = ''
    for var in varlist:
        ret = ret + sep + cprint(var, ctype, width, scale)
        sep = char['space']
    return ret

def header(xs, width):
    "Return the header for a set of module counters"
    line = ''
    ### Process title
    for o in xs:
        line += theme['frame']
        line += o['title'].center(o['width'] + 3, '-')
        line += theme['frame'] + char['space']

    line += '\n'
    for o in xs:
        line += ansi['bold'] + ansi['underline']
        for st in o['subtitle']:
            if st is o['subtitle'][-1]:
                line += st.center(width + 2)
            else:
                line += st.center(width + 1)

        if o is not xs[-1]:
            line += theme['frame'] + char['pipe']
        else:
            line += theme['title']


    line += ansi['reset']

    return line

###################
# QuasarDB
###################


def _parse_user_security_file(x):
    with open(x, 'r') as fp:
        parsed = json.load(fp)
        return (parsed['username'], parsed['secret_key'])


def _slurp(x):
    with open(x, 'r') as fp:
        return fp.read()

def get_conn(uri, cluster_public_key=None, user_security_file=None):
    if cluster_public_key and user_security_file:
        user, private_key = _parse_user_security_file(user_security_file)
        public_key = _slurp(cluster_public_key)
        logger.debug("Establishing secure cluster connection...")

        return quasardb.Cluster(uri,
                                user_name=user,
                                user_private_key=private_key,
                                cluster_public_key=public_key)
    else:
        logger.debug("Establishing insecure cluster connection...")
        return quasardb.Cluster(uri)


def grab_stats(args):
    with get_conn(args.cluster_uri,
                  cluster_public_key=args.cluster_public_key,
                  user_security_file=args.user_security_file) as conn:
        return qdbst.by_node(conn)

def _to_header_vals(xs):
    regex_pipe = re.compile('async_pipelines.pipe_([0-9]+)')
    ret = {}
    for x in xs:
        k = x['key']
        module = x['module']

        if x['module'] == 'async_details':
            matches_pipe = regex_pipe.match(k)
            if not matches_pipe:
                logger.warn("async pipeline without matching regex? {}".format(k))

            pipe_id = int(matches_pipe.groups()[0])
            pipe_key = pipe_id + x['offset']
            pipe_str = 'pipe{}'.format(pipe_id)

            if not pipe_key in ret:
                ret[pipe_key] = {'title': pipe_str,
                                 'subtitle': [],
                                 'width': 0}

            if k.endswith('bytes'):
                ret[pipe_key]['subtitle'].append('byt')
            elif k.endswith('count'):
                ret[pipe_key]['subtitle'].append('cnt')

            ret[pipe_key]['width'] = 5 * len(ret[pipe_key]['subtitle'])

        elif x['module'] == 'async':
            ret[x['offset']] = {'title': 'async merge',
                                'subtitle': ['bkt', 'dur', 'req'],
                                'width': 16}
        elif x['module'] == 'requests':
            ret[x['offset']] = {'title': 'requests',
                                'subtitle': ['in', 'out'],
                                'width': 10}
        elif x['module'] == 'persistence_bytes':
            ret[x['offset']] = {'title': 'pers/bytes',
                                'subtitle': ['read', 'utilized', 'write'],
                                'width': 16}
        elif x['module'] == 'persistence_bucket':
            ret[x['offset']] = {'title': 'pers/buckets',
                                'subtitle': ['insert', 'read', 'update'],
                                'width': 16}

    ret_ = [ret[k] for k in sorted(ret.keys())]
    return ret_


def _print_delta(modules, delta, print_header=False):

    keys  = []

    offset = 0
    for module in modules:
        if module == 'async':
            keys.extend([{'key': k,
                          'module': module,
                          'offset': offset} for k in delta.keys() if k.startswith('async_pipelines.merge.')])
        elif module == 'async_details':
            keys.extend([{'key': k,
                          'module': module,
                          'offset': offset} for k in delta.keys() if k.startswith('async_pipelines.pipe_')])
        elif module == 'requests':
            keys.extend([{'key': k,
                          'module': module,
                          'offset': offset} for k in delta.keys() if k.startswith('requests.bytes_')])
        elif module == 'persistence':
            keys.extend([{'key': k,
                          'module': '{}_bytes'.format(module),
                          'offset': offset} for k in delta.keys() if k.startswith('persistence.bytes_')])
            keys.extend([{'key': k,
                          'module': '{}_bucket'.format(module),
                          'offset': offset + 1} for k in delta.keys() if k.startswith('persistence.bucket_')])
        else:
            print("unrecognized module: {}".format(module))

        offset += 100

    #header = [_to_header(k) for k in keys]
    xs        = [{'delta': delta[k['key']],
                  'module': k['module']}
                 for k in keys]

    col_width = 5
    sep = theme['frame'] + char['colon']

    if print_header is True:
        print(header(_to_header_vals(keys),
                      width=col_width))

    print(cprintlist((x['delta'] for x in xs), ctype='f', width=col_width + 1, scale=1000))

default_modules = ['requests', 'async', 'persistence']

def main():
    args = get_args()
    argsv = vars(args)
    last = None
    i = 0
    enabled_modules = [m['id'] for m in modules if argsv[m['id']] is True]
    if len(enabled_modules) == 0:
        enabled_modules = default_modules

    last_delta = None

    while True:
        cur = grab_stats(args)
        if last is not None:
            delta = qdbst.calculate_delta(last, cur)
            if len(delta.keys()) == 1:
                cur_delta = delta[next(iter(delta))]

                # Printer header once every 25 rows, *or* when we suddenly have more columns
                # in the delta than last time.
                keys_changed = last_delta != None and last_delta.keys() != cur_delta.keys()
                print_header = keys_changed or (i % 25 == 0)


                _print_delta(enabled_modules,
                             cur_delta,
                             print_header=print_header)

                last_delta = cur_delta
                i += 1


        last = cur

        time.sleep(args.interval)




if __name__ == "__main__":
    sys.exit(main())
