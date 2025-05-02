# Redis Producer Consumer

Most simple and minimalistic example of a cross process Redis Producer consumer pattern.

## Install

```bash
git clone git@github.com:grll/redis-producer-consumer.git
cd redis-producer-consumer
uv sync
```

Follow platform specific instruction on official redis sources to install redis on your
os.

## How to run

Make sure redis is up and running first. Also activate uv created virtual env if not
already activated: `source .venv/bin/activate`.

Start the consumer:

```bash
python consumer.py
```

Start the producers:

In a separate shell:

```bash
python process1.py
```

```bash
python process2.py
```
