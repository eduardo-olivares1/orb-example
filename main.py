import sys
import time
from dotenv import load_dotenv
import os
from orb import Orb
import orb
import csv
import logging
import datetime
import uuid

# Set up logging
# Keep it simple
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


# Load environment variables from .env file
load_dotenv()

# Access environment variables
ORB_API_KEY = os.getenv("ORB_API_KEY")


def parse_int(value: str) -> int:
    """Convert a string to an integer, removing commas and returning 0 if the value is empty"""
    if value == "":
        return 0
    return int(value.replace(",", ""))


def main() -> None:
    # Create Orb client
    client = Orb(api_key=ORB_API_KEY)

    with open(
        "data/Orb Technical Support Engineer (TSE) - Technical Exercise - sample_data - sample_data.csv",
        "r",
    ) as file:
        # Read the CSV file of transactions to be ingested as events
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            logger.debug("Processing row: %s", row)
            # Create null customer placeholder
            customer = None
            logger.debug("Checking for customer with account_id: %s", row["account_id"])

            # Try to find customer by external_customer_id
            try:
                customer = client.customers.fetch_by_external_id(
                    external_customer_id=row["account_id"]
                )
            except orb.APIConnectionError as e:
                # If the server could not be reached, exit
                logger.error("The server could not be reached")
                logger.error("Underlying exception: %s", e.__cause__)
                logger.error("Exiting")
                sys.exit(1)
            except orb.RateLimitError as e:
                # If a 429 status code was received, exit and rerun the script later
                logger.error(
                    "A 429 status code was received; we should back off a bit... %s", e
                )
                logger.error("Exiting")
                sys.exit(1)
            except orb.APIStatusError as e:
                # A 404 status code indicates that the customer does not exist, proceed to create
                logger.error("Another non-200-range status code was received")
                if e.status_code == 404:
                    # At this point customer is None
                    logger.error(
                        "Status was 404, customer not found, creating new customer"
                    )
                else:
                    logger.error("Status code: %s", e.status_code)
                    logger.error("Response: %s", e.response.json())
                    logger.error("Exiting")
                    sys.exit(1)
            except Exception as e:
                # If any other error occurs, exit
                logger.error("Error fetching customer: %s", e)
                logger.error("Exiting")
                sys.exit(1)

            # Create customer if they don't exist
            if customer is None:
                logger.debug("Creating customer for account_id: %s", row["account_id"])
                try:
                    # Attempt to create the customer
                    customer = client.customers.create(
                        external_customer_id=row["account_id"],
                        name=row["account_id"].replace("_", " ").title(),  # Prettyfy name for display in the Orb UI
                        email=f"admin@{row['account_id'].replace('_', '-')}.com",  # Create a dummy email for the customer
                        idempotency_key=str(uuid.uuid4()),  # Generate a unique idempotency key for the customer
                    )
                except orb.APIConnectionError as e:
                    # If the server could not be reached, exit
                    logger.error("The server could not be reached")
                    logger.error("Underlying exception: %s", e.__cause__)
                    logger.error("Exiting")
                    sys.exit(1)
                except orb.RateLimitError as e:
                    # If a 429 status code was received, exit and rerun the script later
                    logger.error(
                        "A 429 status code was received; we should back off a bit... %s",
                        e,
                    )
                    logger.error("Exiting")
                    sys.exit(1)
                except orb.APIStatusError as e:
                    # If another non-200-range status code was received, exit
                    logger.error("Another non-200-range status code was received")
                    logger.error("Status code: %s", e.status_code)
                    logger.error("Response: %s", e.response.json())
                    logger.error("Exiting")
                    sys.exit(1)
                except Exception as e:
                    # If any other error occurs, exit
                    logger.error("Error creating customer: %s", e)
                    logger.error("Exiting")
                    sys.exit(1)

            else:
                # If the customer was found, log it
                logger.debug(
                    "Customer found: %s (ID: %s)", row["account_id"], customer.id
                )

            # Create event payload
            # Date must be in ISO format
            # Idempotency should be a UUID v4
            event = {
                "customer_id": customer.id,
                # You can only use customer_id or external_customer_id, not both
                # "external_customer_id": row["account_id"],
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                "idempotency_key": str(uuid.uuid4()),
                "event_name": "payment_transaction",
                "properties": {
                    "transaction_id": row["transaction_id"],
                    "account_type": row["account_type"],
                    "bank_id": row["bank_id"],
                    "standard": parse_int(row["standard"]),
                    "sameday": parse_int(row["sameday"]),
                    "month": row["month"],
                },
            }

            logger.debug("Attempting to ingest event: %s", event)

            # Create null response placeholder
            response = None

            # Dont exit on any errors, just log them and move on
            try:
                # Attempt to ingest the event
                response = client.events.ingest(events=[event])

            except orb.APIConnectionError as e:
                logger.error("The server could not be reached")
                logger.error("Underlying exception: %s", e.__cause__)
            except orb.RateLimitError as e:
                logger.error(
                    "A 429 status code was received; we should back off a bit... %s", e
                )
            except orb.APIStatusError as e:
                logger.error("Another non-200-range status code was received")
                logger.error("Status code: %s", e.status_code)
                logger.error("Response: %s", e.response.json())

            except Exception as e:
                logger.error("Error ingesting event: %s", e)

            # Sleep for 1.5 seconds to avoid rate limiting
            if response is not None:
                logger.debug(
                    "Successfully ingested event for transaction %s",
                    row["transaction_id"],
                )
                logger.debug("Response: %s", response)
            else:
                logger.error(
                    "Failed to ingest event for transaction %s", row["transaction_id"]
                )

            # Sleep for 1.5 seconds to avoid rate limiting
            time.sleep(1.5)

    logger.debug("Closing Orb client")
    # Not sure if this is necessary but it seems like a graceful exit
    client.close()
    logger.debug("Finished ingesting events")


if __name__ == "__main__":
    main()
