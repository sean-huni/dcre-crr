# CRR is the single writer of the collections spine (R-04): it reads one
# OnHost collection request file per arrival and persists the header plus
# every detail line, keyed (arrival_id, sequence) for restart safety (R-05).
@crr
Feature: CRR boundary reader ingests OnHost collection request files
  The boundary reader parses a fixed-width collection request file, applies
  the structural file-fatal tier (R-19) and persists the transaction spine.

  Scenario: A valid V2 collection file is persisted into the transaction spine
    Given the boundary reader receives the standard V2 collection file
    When the CRR job runs
    Then the job completes with a clean business verdict
    And the spine holds one header row and 30 entry rows for the arrival
    And every persisted amount equals its raw digits scaled to two decimals

  Scenario: Re-processing the same arrival leaves the spine unchanged
    Given the boundary reader receives the standard V2 collection file
    When the CRR job runs
    And the CRR job runs again for the same arrival
    Then the job completes with a clean business verdict
    And the spine holds one header row and 30 entry rows for the arrival

  Scenario: A pay-flow launch stamps the header with the PAY flow
    Given the boundary reader receives the standard V2 collection file
    When the CRR job runs with the launch flow "PAY"
    Then the job completes with a clean business verdict
    And the header row is stamped with flow "PAY"

  Scenario: A launch without a flow parameter defaults the header to collections
    Given the boundary reader receives the standard V2 collection file
    When the CRR job runs
    Then the job completes with a clean business verdict
    And the header row is stamped with flow "COL"

  Scenario: A header declaring the wrong transaction count is rejected file-fatally
    Given the boundary reader receives the V2 file with a header declaring 31 transactions
    When the CRR job runs
    Then the file is rejected file-fatally with a reason containing "tx_count=31"
    And no spine entries are persisted for the arrival

  Scenario: A malformed short header is rejected file-fatally
    Given the boundary reader receives the V2 file with a truncated header
    When the CRR job runs
    Then the file is rejected file-fatally with a reason containing "header shorter"
    And no spine entries are persisted for the arrival

  Scenario: A filename contradicting the header destination is rejected file-fatally
    Given the boundary reader receives the standard V2 collection file
    And the file arrived under the name "FNBXX99_DCRERF2026071112000002.txt"
    When the CRR job runs
    Then the file is rejected file-fatally with a reason containing "R-31 mismatch"
    And no spine entries are persisted for the arrival

  Scenario: The legacy V1 layout fails closed while disabled by default
    Given the boundary reader receives the legacy V1 collection file
    When the CRR job runs
    Then the file is rejected file-fatally with a reason containing "V1 layout fails closed"
    And no spine entries are persisted for the arrival
