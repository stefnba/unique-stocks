import { QueryFile } from 'pg-promise';

export {};

declare global {
    namespace Errors {
        interface Options {
            cause?: Error;
        }
        interface DatabaseErrorConfig {
            query: string | QueryFile;
            code: string;
        }
        interface ServiceErrorConfig {
            code: string;
            httpStatus: number;
        }
        interface ErrorOptions {
            user?: string;
            hint?: string;
            cause?: Error;
        }
        interface ValidationErrorConfig {
            field: string;
            value: unknown;
            error: string;
            message: string;
            publicMessage: string;
        }
    }

    /**
     * Make certain types optional
     */
    type Optional<T, K extends keyof T> = Pick<Partial<T>, K> & Omit<T, K>;
    /**
     * Pick only certain types, make some of them optional
     */
    type PickPartial<
        T,
        S extends keyof T,
        O extends keyof Pick<T, S>
    > = Optional<Pick<T, S>, O>;

    type Swap<T, R extends keyof T, A> = Omit<T, R> & A;
}
