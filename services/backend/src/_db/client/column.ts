import pgPromise, { IColumnDescriptor } from 'pg-promise';
import type { ColumnSetParams } from './types';

const PgColumnSet = pgPromise().helpers.ColumnSet;

export class ColumnSet<M = undefined> extends PgColumnSet<M> {
    constructor(columns: ColumnSetParams<M>, table?: string) {
        const _columns = columns.map((col) => {
            if (typeof col === 'string') {
                // make optional if ? is provided in column name
                if (col.endsWith('?')) {
                    return {
                        name: col.replace('?', ''),
                        skip: (a: IColumnDescriptor<never>) => !a.exists
                    };
                }
                return col;
            }
            if (typeof col === 'object' && 'optional' in col) {
                const { optional, ...rest } = col;
                if (optional) {
                    return {
                        ...rest,
                        skip: (c: IColumnDescriptor<never>) => !c.exists
                    };
                }
                return rest;
            }
            return col;
        });

        super(_columns, { table });
    }
}
