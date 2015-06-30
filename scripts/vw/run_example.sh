 #!/bin/bash

set -o nounset
set -o errexit
set -v

#examples:

# oaa 
echo "1 |f f1:1 f2:2" | ./vw_hash.py --oaa 3

# binary example with bit precision of 19
echo "1 |f f1:1 f2:2" | ./vw_hash.py -b 19






