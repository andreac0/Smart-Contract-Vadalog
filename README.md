# Smart-Contract-Vadalog

This repository contains the codes needed to replicate the Kwenta ETH-PERP Smart Contract (https://optimistic.etherscan.io/address/0xf86048dff23cf130107dfb4e6386f574231a5c65#code).

In particular:
- Data extraction and decodification of blockchain data is done by the ETL_history.py file, which retrieves for the desired interval all inputs (and all benchmark outputs) needed to run the Vadalog program
- Vadalog implementation of the contract inside eth_perpetual.ipynb: a notebook which contains the concrete implementation in Vadalog
