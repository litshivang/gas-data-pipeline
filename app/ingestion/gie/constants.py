DATASET_AGSI = "AGSI"
DATASET_ALSI = "ALSI"

SOURCE_AGSI = "GIE_AGSI"
SOURCE_ALSI = "GIE_ALSI"

DELETE_LOOKBACK_DAYS = 10

EXCLUDED_KEYS = {
    "name",
    "code",
    "url",
    "updatedAt",
    "gasDayStart",
    "gasDayEnd",
    "info",
}

NULL_LIKE_VALUES = {"", " ", None}
