#include "postgres.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

// Core Structure for `tjsonb`
typedef struct {
    TimestampTz timestamp;
    Jsonb *jsondata;
} TJsonb;

// Function to compare two JsonbValue items for containment
bool compare_jsonb_values(JsonbValue *v1, JsonbValue *v2) {
    if (v1->type != v2->type) {
        return false;
    }
    switch (v1->type) {
        case jbvString:
            return v1->val.string.len == v2->val.string.len &&
                   strncmp(v1->val.string.val, v2->val.string.val, v1->val.string.len) == 0;
        case jbvNumeric:
            return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
                                  NumericGetDatum(v1->val.numeric),
                                  NumericGetDatum(v2->val.numeric))) == 0;
        case jbvBool:
            return v1->val.boolean == v2->val.boolean;
        default:
            return false; // Only basic types are handled here
    }
}

// Function to check JSON containment by comparing JSON values manually
bool jsonb_contains_container(JsonbContainer *container1, JsonbContainer *container2) {
    JsonbIterator *it1 = JsonbIteratorInit(container1);
    JsonbIterator *it2 = JsonbIteratorInit(container2);
    JsonbValue v1, v2;
    JsonbIteratorToken tok1, tok2;

    while ((tok1 = JsonbIteratorNext(&it1, &v1, false)) != WJB_DONE &&
           (tok2 = JsonbIteratorNext(&it2, &v2, false)) != WJB_DONE) {
        if (tok1 != tok2 || !compare_jsonb_values(&v1, &v2)) {
            return false; // Not contained if structure or values differ
        }
    }

    return true;
}

// Input Function
PG_FUNCTION_INFO_V1(tjsonb_in);
Datum
tjsonb_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    TJsonb *result = (TJsonb *) palloc(sizeof(TJsonb));
    char *delimiter = strchr(str, '|');

    if (delimiter == NULL)
        ereport(ERROR, (errmsg("Invalid input format for tjsonb")));

    *delimiter = '\0';
    char *timestamp_str = str;
    char *json_str = delimiter + 1;

    result->timestamp = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
                                CStringGetDatum(timestamp_str),
                                ObjectIdGetDatum(InvalidOid),
                                Int32GetDatum(-1)));

    result->jsondata = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json_str)));

    PG_RETURN_POINTER(result);
}

// Output Function
PG_FUNCTION_INFO_V1(tjsonb_out);
Datum
tjsonb_out(PG_FUNCTION_ARGS)
{
    TJsonb *tjsonb = (TJsonb *) PG_GETARG_POINTER(0);
    char *timestamp_str = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(tjsonb->timestamp)));
    char *json_str = JsonbToCString(NULL, &tjsonb->jsondata->root, VARSIZE(tjsonb->jsondata));
    char *result = psprintf("%s|%s", timestamp_str, json_str);

    PG_RETURN_CSTRING(result);
}

// Overlap Detection Function
PG_FUNCTION_INFO_V1(tjsonb_overlaps);
Datum
tjsonb_overlaps(PG_FUNCTION_ARGS)
{
    TJsonb *first = (TJsonb *) PG_GETARG_POINTER(0);
    TJsonb *second = (TJsonb *) PG_GETARG_POINTER(1);

    if (first->timestamp == second->timestamp &&
        jsonb_contains_container(&first->jsondata->root, &second->jsondata->root)) {
        PG_RETURN_BOOL(true);
    } else {
        PG_RETURN_BOOL(false);
    }
}

// Aggregation Function for Average Speed
PG_FUNCTION_INFO_V1(tjsonb_aggregate_speed);
Datum
tjsonb_aggregate_speed(PG_FUNCTION_ARGS)
{
    ArrayType *input_array = PG_GETARG_ARRAYTYPE_P(0);
    int num_elements = ArrayGetNItems(ARR_NDIM(input_array), ARR_DIMS(input_array));
    TJsonb **elements = (TJsonb **) ARR_DATA_PTR(input_array);
    float8 total_speed = 0;
    int count = 0;

    for (int i = 0; i < num_elements; i++) {
        Jsonb *json = elements[i]->jsondata;
        JsonbContainer *jsonContainer = &json->root;
        JsonbValue res;
        JsonbValue key;

        key.type = jbvString;
        key.val.string.val = "speed";
        key.val.string.len = strlen("speed");

        if (JsonbExtractScalar(jsonContainer, &res) && res.type == jbvNumeric) {
            total_speed += DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(res.val.numeric)));
            count++;
        }
    }

    if (count == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_FLOAT8(total_speed / count);
    }
}

// Range Detection Function
PG_FUNCTION_INFO_V1(tjsonb_range);
Datum
tjsonb_range(PG_FUNCTION_ARGS)
{
    TJsonb *start = (TJsonb *) PG_GETARG_POINTER(0);
    TJsonb *end = (TJsonb *) PG_GETARG_POINTER(1);
    
    if (start->timestamp <= end->timestamp &&
        jsonb_contains_container(&start->jsondata->root, &end->jsondata->root)) {
        PG_RETURN_TEXT_P(cstring_to_text("Overlap found within range"));
    } else {
        PG_RETURN_TEXT_P(cstring_to_text("No overlap in specified range"));
    }
}
