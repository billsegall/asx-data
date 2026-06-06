# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Fine-tune Kronos predictor on ASX OHLCV (continued pre-training, causal LM objective).

Tokenizer is frozen; only the predictor (Kronos) is updated.
Run from asx-data/ root:
    python -m analysis.cli.finetune_kronos --db stockdb/stockdb.db
"""

import argparse
import os
import sys
import time

import torch
from torch.utils.data import DataLoader


def _import_kronos(kronos_src: str):
    try:
        from model import Kronos, KronosTokenizer
        return Kronos, KronosTokenizer
    except ImportError:
        if kronos_src not in sys.path:
            sys.path.insert(0, kronos_src)
        from model import Kronos, KronosTokenizer
        return Kronos, KronosTokenizer


def main():
    parser = argparse.ArgumentParser(description='Fine-tune Kronos on ASX OHLCV')
    parser.add_argument('--db', required=True, help='Path to stockdb.db')
    parser.add_argument('--model-dir',
                        default='analysis/kronos/weights/kronos-mini',
                        help='Pretrained Kronos predictor weights dir')
    parser.add_argument('--tokenizer-dir',
                        default='analysis/kronos/weights/tokenizer',
                        help='Pretrained KronosTokenizer weights dir')
    parser.add_argument('--out-dir',
                        default='analysis/kronos/weights/kronos-mini-asx',
                        help='Output dir for fine-tuned weights')
    parser.add_argument('--epochs',     type=int,   default=10)
    parser.add_argument('--batch-size', type=int,   default=128)
    parser.add_argument('--lr',         type=float, default=4e-5)
    parser.add_argument('--lookback',   type=int,   default=128)
    parser.add_argument('--predict-len',type=int,   default=10)
    parser.add_argument('--workers',    type=int,   default=4)
    args = parser.parse_args()

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}")

    here = os.path.dirname(os.path.abspath(__file__))
    kronos_src = os.path.join(here, '..', 'kronos', 'Kronos')
    Kronos, KronosTokenizer = _import_kronos(os.path.abspath(kronos_src))

    from analysis.kronos.loader import load_all_ohlcv
    from analysis.kronos.asx_dataset import ASXKronosDataset

    print("Loading OHLCV...")
    ohlcv = load_all_ohlcv(args.db)

    train_ds = ASXKronosDataset(
        args.db, split='train', lookback=args.lookback,
        predict_len=args.predict_len, ohlcv=ohlcv,
    )
    val_ds = ASXKronosDataset(
        args.db, split='val', lookback=args.lookback,
        predict_len=args.predict_len, ohlcv=ohlcv,
    )

    train_loader = DataLoader(
        train_ds, batch_size=args.batch_size, shuffle=True,
        num_workers=args.workers, pin_memory=True,
    )
    val_loader = DataLoader(
        val_ds, batch_size=args.batch_size, shuffle=False,
        num_workers=args.workers, pin_memory=True,
    )

    print(f"Loading tokenizer from {args.tokenizer_dir}")
    tokenizer = KronosTokenizer.from_pretrained(args.tokenizer_dir).to(device)
    tokenizer.eval()
    for p in tokenizer.parameters():
        p.requires_grad_(False)

    print(f"Loading model from {args.model_dir}")
    model = Kronos.from_pretrained(args.model_dir).to(device)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"Model parameters: {n_params:,}")

    # Verify tokenizer.encode() shape before committing to a full run
    window = args.lookback + args.predict_len + 1
    dummy = torch.zeros(2, window, 6, device=device)
    with torch.no_grad():
        t0, t1 = tokenizer.encode(dummy, half=True)
    print(f"Tokenizer encode OK: token shapes {t0.shape}, {t1.shape}")

    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=1e-4)
    total_steps = args.epochs * len(train_loader)
    scheduler = torch.optim.lr_scheduler.OneCycleLR(
        optimizer, max_lr=args.lr, total_steps=total_steps, pct_start=0.1,
    )

    os.makedirs(args.out_dir, exist_ok=True)
    best_val_loss = float('inf')

    for epoch in range(args.epochs):
        t_epoch = time.time()
        model.train()
        train_loss, n_train = 0.0, 0

        for batch_x, batch_x_stamp in train_loader:
            batch_x       = batch_x.to(device, non_blocking=True)
            batch_x_stamp = batch_x_stamp.to(device, non_blocking=True)

            with torch.no_grad():
                token_seq_0, token_seq_1 = tokenizer.encode(batch_x, half=True)

            token_in  = [token_seq_0[:, :-1], token_seq_1[:, :-1]]
            token_out = [token_seq_0[:, 1:],  token_seq_1[:, 1:]]

            logits = model(token_in[0], token_in[1], batch_x_stamp[:, :-1, :])
            loss, _, _ = model.head.compute_loss(
                logits[0], logits[1], token_out[0], token_out[1]
            )

            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=3.0)
            optimizer.step()
            scheduler.step()

            train_loss += loss.item()
            n_train += 1

        model.eval()
        val_loss, n_val = 0.0, 0
        with torch.no_grad():
            for batch_x, batch_x_stamp in val_loader:
                batch_x       = batch_x.to(device, non_blocking=True)
                batch_x_stamp = batch_x_stamp.to(device, non_blocking=True)

                token_seq_0, token_seq_1 = tokenizer.encode(batch_x, half=True)
                token_in  = [token_seq_0[:, :-1], token_seq_1[:, :-1]]
                token_out = [token_seq_0[:, 1:],  token_seq_1[:, 1:]]

                logits = model(token_in[0], token_in[1], batch_x_stamp[:, :-1, :])
                loss, _, _ = model.head.compute_loss(
                    logits[0], logits[1], token_out[0], token_out[1]
                )
                val_loss += loss.item()
                n_val += 1

        avg_train = train_loss / n_train
        avg_val   = val_loss   / n_val
        elapsed   = time.time() - t_epoch
        print(f"Epoch {epoch+1:2d}/{args.epochs}  train={avg_train:.4f}  val={avg_val:.4f}  {elapsed:.0f}s")

        if avg_val < best_val_loss:
            best_val_loss = avg_val
            model.save_pretrained(args.out_dir)
            print(f"  -> best saved (val={best_val_loss:.4f})")

    print(f"\nDone. Best val={best_val_loss:.4f}  Weights: {args.out_dir}")


if __name__ == '__main__':
    main()
