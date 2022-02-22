# **Privacy-Enhanced Trading Exchange (PET-Exchange)**

An implementation of a cryptographically private trading scheme implemented using homomorphic encryption and padding. The goal of the project is to create a scheme for creating privacy for a trade from creation to execution.

## **Plaintext Trading**

---

A trade is usually entered from a broker or some party either directly or on behalf of a client, when arriving to the exchange the trade is commonly handled in plaintext, that is anyone with access to the order book can read and get information about others trades before they have been executed. This sort of plaintext trading is beneficial in the sense that the trading and the handling of the trade is fully transparent for all participants in the market. However, it also allows for anyone to know that a specific party is interested in buying or selling their stake in a traded security, this is a significant information disclosure for most parties and could lead to unethical trading practices where actors exploit the transparency of the market for their own gain. Such exploits has increased over the years with the increased usage of high-frequency trading with computers that can act on any market information faster than any individual could, this creates an unfairness to what is supposed to be a fair market. In order to hide parties interest in particular securities some band together to create or participate in _dark pools_ which is an off-exchange marketplace where parties publish trades after they have been executed, so if a trade is not fulfilled then it won't be known outside the dark pool. Oftentimes these dark pools are limited to high-frequency traders or traders with a certain liquidity further increasing the unfairness in the market as these securities are not available for smaller traders.

## **Cryptographic Trading**

---

A solution to this issue is to limit the transparency in the market, whilst this has consequences for the transparency of the execution it helps prevent unfair advantages that certain actors have. This project aims to implement such a restriction where no market participant has an advantage as no one knows the exact trade before it has been executed and made public. The prototype utilizes advances in _privacy-enhancing technologies_ specifically in the field of _homomorphic encryption_ (HE) in order to operate on encrypted orders just as if they were plaintext orders. Operations on homomorphically encrypted data is quite expensive resource wise so the performance of the exchange is quite affected by this addition. However, by encrypting the orders from start then the exchange won't know the volume and price for the order and no other participant will know either except for the party who published the order, so any other participant such as high-frequency traders or algo-traders won't be able to take advantage of the trades before they have been executed just as any other trader. The prototype also aims to support partial order filling meaning that an order stays secret until it has been fully filled but can still be partially filled without revealing the entire order.

## **Building**

---

In order to build and run the project you need to have the following:

-   Python >= 3.9 (Might work on older versions but not tested)
-   Make

The components use `gRPC` to communicate which means you have to compile the protocol specifications as part of the `Makefile`.

1. `make develop`

or

1. `python setup.py develop`
2. `python -m grpc_tools.protoc -Ipet_exchange/exchange/proto/ --python_out=./pet_exchange/proto --grpc_python_out=./pet_exchange/proto ./pet_exchange/exchange/proto/exchange.proto`
3. `python -m grpc_tools.protoc -Ipet_exchange/intermediate/proto/ --python_out=./pet_exchange/proto --grpc_python_out=./pet_exchange/proto ./pet_exchange/intermediate/proto/intermediate.proto`

## **Examples**

---

### **Basic Usage**

In `./examples/orders.json` you can see the example structure used as input to the. The input consists of a client name as a key and the orders that the client publishes as an array. Each item in the array describes the instrument to be traded, the type to use, the volume and the price. All off these items will be published to the exchange.

In order to run the evaluation of the order data run the following:

-   `python pet_exchange --exchange-server --intermediate-server --client-input-files examples/orders.json --exchange-output trades/exchange.json --client-output trades/clients.json`
    -   Note: You must create a valid path (`trades/`) to the output files, otherwise the framework will fail to run. The output file will be created, but the directories must exist.

or it's short form

-   `python pet_exchange -e:s -i:s -c:i examples/orders.json -e:o trades/exchange.json -c:o trades/clients.json`

The above example will start the exchange and the intermediate on your local host as indicated by `--exchange-server` and `--intermediate-server`, and you will also act as a client or multiple clients based on the input from `--client-input-files`. By default the servers are run from ports `50050` and `50051`.

### **Distributed Usage**

PET-Exchange is designed to only communicate over `gRPC` between components, so this makes it possible to run one component on one host and another component on another host.

For example on host `ABC`

-   `python pet_exchange -e:s -e:o trades/exchange.json -i:h [CBA_IP] -i:p [CBA_PORT]`

The command will then host the exchange server on host `ABC` and then you can do the following on another host `CBA`

-   `python pet_exchange -i:s -e:h [ABC_IP] -e:p [ABC_PORT]`

Which will start the intermediate server and let it know to us the exchange on the host `ABC`

Finally we can start issuing client orders on a third host `BCA`

-   `python pet_exchange -c:i examples/orders.json -c:o trades/clients.json -e:h [ABC_IP] -e:p [ABC_PORT] -i:h [CBA_IP] -i:p [CBA_PORT]`

Which will start sending out the client orders to the exchange.

Note that the intermediate is only used when sending encrypted orders.

## **System Overview**

---

TODO
