/**
 * Capitalize values of certain keys.
 * @param fields
 * @param object
 */
export const capitalizeKeyFields = (
    fields: string[],
    object: { [x: string]: string }
) => {
    fields.forEach((field) => {
        if (field in object && object[field]) {
            const value = object[field];
            object[field] = value.toUpperCase();
        }
    });
};

/**
 * Ensures that every log record in db has the following properties as null
 * @param fields
 * @param object
 */
export const populateRequiredFields = (
    fields: string[],
    object: { [x: string]: unknown }
) => {
    fields.forEach((field) => {
        if (!(field in object)) {
            object[field] = null;
        }
    });
};
