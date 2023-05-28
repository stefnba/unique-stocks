import { QueryFile } from 'pg-promise';
import path from 'path';
import { QueryBuildError } from './error';

const joinPath = path.join;

/**
 * Creates new SQL QueryFile from path provided
 * @param path
 * @param directory
 * @returns
 */
export const sqlFile = (
    path: string | string[],
    directory?: string | string[]
): QueryFile => {
    const filePath = buildPath(path);
    const baseDirPath = buildPath(directory || process.cwd());

    const qf = new QueryFile(joinPath(baseDirPath, filePath), {
        minify: true,
        debug: true // todo env conditon
    });

    if (qf.error) {
        const path = (qf.error as Error & { file: string }).file;
        throw new QueryBuildError({
            message: `SQL file "${path}" does not exist`,
            type: 'SQL_FILE_NOT_FOUND'
        });
    }

    return qf;
};

/**
 * Joins together paths
 * @param path
 * Location parts where file is stored
 * @returns
 * Complete joined path
 */
function buildPath(path: string | string[] | undefined): string {
    if (!path) return '';
    return Array.isArray(path) ? joinPath(...path) : path;
}
