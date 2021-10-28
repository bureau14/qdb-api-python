import argparse
import logging
import sys
import time
import re

import statistics
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
            'arg': '--async-details'}]

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

def dchg(var, width, base):
    "Convert decimal to string given base and length"
    c = 0
    while True:
        ret = str(int(round(var)))
        if len(ret) <= width:
            break
        var = var / base
        c = c + 1
    else:
        c = -1
    return ret, c

def tchg(var, width):
    "Convert time string to given length"
    ret = '%2dh%02d' % (var / 60, var % 60)
    if len(ret) > width:
        ret = '%2dh' % (var / 60)
        if len(ret) > width:
            ret = '%2dd' % (var / 60 / 24)
            if len(ret) > width:
                ret = '%2dw' % (var / 60 / 24 / 7)
    return ret

def cprint(var, ctype = 'f', width = 4, scale = 1000):
    "Color print one column"

    base = 1000
    if scale == 1024:
        base = 1024

    ### Use units when base is exact 1000 or 1024
    unit = False

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
    ret = color + ret.rjust(width)

    ### Add unit to output
    if unit:
        if c != -1 and round(var) != 0:
            ret += cunit + units[c]
        else:
            ret += char['space']

    return ret

def row(xs_by_module, width):

    ret = ''

    sep = ''
    for m in xs_by_module.values():
        ret += sep

        for k in m['keys']:
            ctype = 'd'
            if 'ctype' in k:
                ctype = k['ctype']

            ret += sep + cprint(k['value'], ctype, width, scale=1000)
            sep = char['space']

        ret += sep

    return ret

def header(xs_by_module, width):
    line = ''

    ### Process title
    first = True
    for module in xs_by_module:
        m = xs_by_module[module]

        keys = len(m['keys'])
        title_width = (len(m['keys']) * (width + 1))
        if first is False:
            title_width += 1
        first = False

        line += theme['frame']
        line += m['title'].center(title_width, '-')
        line += theme['frame'] + char['space']

    line += '\n'
    first = True
    for module in xs_by_module:
        m = xs_by_module[module]


        line += ansi['bold'] + ansi['underline']
        subtitles = [o['subtitle'] for o in m['keys']]

        for st in subtitles:
            line += st.center(width + 1)

        if first is True:
            line += theme['frame']
        else:
            line += ' ' + theme['title']

        first = False
        line +=  char['pipe']

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
        return qdbst.of_node(conn, args.node_id)


prefix_by_module = {'async': 'async_pipelines.merge.',
                    'async_details': 'async_pipelines.pipes',
                    'requests': 'requests.bytes_',
                    'persistence': ['persistence.bytes_',
                                    'persistence.bucket_']}


title_by_module = {'async': 'async',
                   'async_details': 'async pipes',
                   'async_details_bytes': 'async pipes (bytes)',
                   'async_details_count': 'async pipes (count)',
                   'requests': 'requests',
                   'persistence': 'persistence',
                   'persistence_bytes': 'persistence (byte)',
                   'persistence_bucket': 'persistence (bucket)',
                   }

props_by_key = {'requests.bytes_in': {'subtitle': 'in'},
                'requests.bytes_out': {'subtitle': 'out'},
                'persistence.bucket_insert_count': {'submodule': 'persistence_bucket',
                                                    'subtitle': 'in'},
                'persistence.bucket_read_count': {'submodule': 'persistence_bucket',
                                                  'subtitle': 're'},
                'persistence.bucket_update_count': {'submodule': 'persistence_bucket',
                                                    'subtitle': 'up'},
                'persistence.bytes_read': {'submodule': 'persistence_bytes',
                                           'subtitle': 'read'},
                'persistence.bytes_utilized': {'submodule': 'persistence_bytes',
                                               'subtitle': 'util'},
                'persistence.bytes_written': {'submodule': 'persistence_bytes',
                                              'subtitle': 'writ'},

                'async_pipelines.merge.bucket_count': {'subtitle': 'bkts'},
                'async_pipelines.merge.duration_us': {'subtitle': 'dur'},
                'async_pipelines.merge.requests_count': {'subtitle': 'req'},

                'async_pipelines.pipes.bytes.mean': {'submodule': 'async_details_bytes',
                                                     'subtitle': 'mean'},
                'async_pipelines.pipes.bytes.median': {'submodule': 'async_details_bytes',
                                                       'subtitle': 'median'},
                'async_pipelines.pipes.bytes.min': {'submodule': 'async_details_bytes',
                                                    'subtitle': 'min'},
                'async_pipelines.pipes.bytes.max': {'submodule': 'async_details_bytes',
                                                    'subtitle': 'max'},

                'async_pipelines.pipes.count.mean': {'submodule': 'async_details_count',
                                                     'subtitle': 'mean'},
                'async_pipelines.pipes.count.median': {'submodule': 'async_details_count',
                                                       'subtitle': 'median'},
                'async_pipelines.pipes.count.min': {'submodule': 'async_details_count',
                                                    'subtitle': 'min'},
                'async_pipelines.pipes.count.max': {'submodule': 'async_details_count',
                                                    'subtitle': 'max'}


                }

def _print_delta(modules, delta, print_header=False):

    delta_by_module  = {}

    for module in modules:


        prefix = prefix_by_module[module]

        # Always coerce prefix to a list
        if type(prefix) == str:
            prefix = [prefix]

        for k in delta.keys():
            for p in prefix:
                if k.startswith(p):
                    props = props_by_key[k].copy()

                    module_id = module
                    if 'submodule' in props:
                        module_id = props['submodule']

                    if not module_id in delta_by_module:
                        delta_by_module[module_id] = {'title': title_by_module[module_id],
                                                      'keys': []}

                    props['key'] = k
                    props['value'] = delta[k]

                    delta_by_module[module_id]['keys'].append(props)

    col_width = 6
    sep = theme['frame'] + char['colon']

    if print_header is True:
        print(header(delta_by_module,
                     width=col_width))

    # ctype = 'f'
    # scale = 1000
    print(row(delta_by_module, width=col_width))

default_modules = ['requests', 'async', 'persistence']

def _derive_async_pipeline_metrics(xs):
    metrics = ['bytes', 'count']

    ret = {}

    stats = {'mean': statistics.mean,
             'median': statistics.median,
             'min': min,
             'max': max}

    for metric_id in metrics:
        vals = [xs[pipe_id][metric_id] for pipe_id in xs]

        for stat_id in stats:
            stat_fn = stats[stat_id]

            ret['async_pipelines.pipes.{}.{}'.format(metric_id, stat_id)] = stat_fn(vals)

    return ret

def _preprocess_delta_async_pipelines(xs):
    regex_pipe = re.compile('async_pipelines.pipe_([0-9]+).merge_map.(bytes|count)')

    ks = []
    pipelines = {}

    for k in xs:
        matches = regex_pipe.match(k)
        if not matches:
            continue

        pipe_id = int(matches.group(1))
        metric_id = matches.group(2)
        metric_value = xs[k]

        if not pipe_id in pipelines:
            pipelines[pipe_id] = {}

        pipelines[pipe_id][metric_id] = metric_value
        ks.append(k)


    for k in ks:
        del xs[k]


    derived = _derive_async_pipeline_metrics(pipelines)
    for k in derived:
        xs[k] = derived[k]

    return xs

def _preprocess_delta(xs):
    return _preprocess_delta_async_pipelines(xs)

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
            delta = _preprocess_delta(qdbst.calculate_delta(last, cur))
            cur_delta = delta

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
