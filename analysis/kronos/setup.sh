#!/bin/bash
# Install Kronos and download weights on the GPU machine (realiti).
# Run once from the asx-data repo root: bash analysis/kronos/setup.sh

set -e
cd "$(dirname "$0")/../.."

echo "==> Cloning Kronos..."
if [[ ! -d analysis/kronos/Kronos ]]; then
    git clone https://github.com/shiyu-coder/Kronos.git analysis/kronos/Kronos
else
    echo "    Already cloned."
fi

echo ""
echo "==> Installing Kronos dependencies..."
pip3 install -r analysis/kronos/Kronos/requirements.txt

echo ""
echo "==> Adding Kronos to Python path (symlink model.py to analysis/kronos/)..."
ln -sf "$(pwd)/analysis/kronos/Kronos/model" analysis/kronos/model 2>/dev/null || true

echo ""
echo "==> Downloading Kronos-mini weights..."
mkdir -p analysis/kronos/weights
python3 -c "
from huggingface_hub import snapshot_download
print('  Downloading Kronos-Tokenizer-base...')
snapshot_download('NeoQuasar/Kronos-Tokenizer-base', local_dir='analysis/kronos/weights/tokenizer')
print('  Downloading Kronos-mini...')
snapshot_download('NeoQuasar/Kronos-mini', local_dir='analysis/kronos/weights/kronos-mini')
print('Done.')
"

echo ""
echo "==> Verifying install..."
python3 -c "
import sys
sys.path.insert(0, 'analysis/kronos/Kronos')
from model import Kronos, KronosTokenizer, KronosPredictor
tok = KronosTokenizer.from_pretrained('analysis/kronos/weights/tokenizer')
mdl = Kronos.from_pretrained('analysis/kronos/weights/kronos-mini')
print(f'  Model params: {sum(p.numel() for p in mdl.parameters()):,}')
print('  OK')
"

echo ""
echo "Setup complete. Run evaluation:"
echo "  python3 -m analysis.cli.run_kronos_ic --db stockdb/stockdb.db \\"
echo "    --model-dir analysis/kronos/weights/kronos-mini \\"
echo "    --tokenizer-dir analysis/kronos/weights/tokenizer"
