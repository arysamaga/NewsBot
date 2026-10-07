"""Entry point: `jac run` starts the newsstream service (see app.jac).

Kept to these few lines on purpose. jac re-analyses the entry module on every
start but restores imported modules from its cache, so launching through this
file brings the service up in about 5 s instead of about 17 s.
"""

import from app { main }

with entry:__main__ {
    main();
}
