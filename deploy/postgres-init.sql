-- Each service owns its own database. There are no cross-service joins and no
-- distributed transactions: consistency between them is eventual, carried by
-- events, and reconciled by each service's own invariants.
CREATE DATABASE wallet;
CREATE DATABASE blockchain;

-- Separate databases for the integration test suite so a test run can never
-- touch development data.
CREATE DATABASE wallet_test;
CREATE DATABASE blockchain_test;
