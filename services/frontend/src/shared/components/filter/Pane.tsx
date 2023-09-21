import React, { useEffect } from 'react';
import { Space, Button } from 'antd';
import { useSearchParams } from 'react-router-dom';
import { useAppSelector } from '@redux';
import { usePaginationFromSearchParams } from '@shared/hooks/pagination';

import queryString, { ParsedQuery } from 'query-string';
import type {
    FilteringActionPayload,
    FilterComponents
} from '@features/filtering/slice.types';
import { actions as filterActions } from '@features/filtering';
import { actions as paginationActions } from '@features/pagination';
import { paginationDefault } from '@features/pagination/slice';

type FilterPaneProps = {
    component: FilterComponents;
};

import { useAppDispatch } from '@redux';

export default function FilterPane({
    children,
    component
}: React.PropsWithChildren & FilterPaneProps) {
    const dispatch = useAppDispatch();
    const pag = usePaginationFromSearchParams();
    let [searchParams, setSearchParams] = useSearchParams();

    const appliedFilters = useAppSelector(
        (state) => state.filtering[component]
    );
    const pagination = useAppSelector((state) => state.pagination[component]);

    const paginationSet = {
        page:
            pagination.page === paginationDefault.page ? null : pagination.page,
        pageSize:
            pagination.pageSize === paginationDefault.pageSize
                ? null
                : pagination.pageSize
    };

    // update url when filter is set
    useEffect(() => {
        if (Object.keys(appliedFilters).length > 0) {
            setSearchParams(
                `?${queryString.stringify(
                    {
                        ...appliedFilters,
                        ...paginationSet
                    },
                    {
                        skipNull: true
                    }
                )}`
            );
        }
    }, [appliedFilters]);
    useEffect(() => {
        if (pagination != paginationDefault) {
            setSearchParams(
                `?${queryString.stringify(
                    {
                        ...appliedFilters,
                        ...paginationSet
                    },
                    {
                        skipNull: true
                    }
                )}`
            );
        }
    }, [pagination]);

    // set filter based on url on mount
    useEffect(() => {
        const search = queryString.parse(searchParams.toString(), {});
        dispatch(
            filterActions.apply({
                component,
                filters: search
            })
        );
        if (pag.page || pag.pageSize) {
            dispatch(
                paginationActions.change({
                    component: 'security',
                    page: pag.page,
                    pageSize: pag.pageSize
                })
            );
        }

        // reset on unmount
        return () => reset();
    }, []);

    const reset = () => {
        dispatch(filterActions.apply({ component, filters: {} }));
        setSearchParams();
    };

    return (
        <Space className="mb-8">
            {/* Filters */}
            {React.Children.map(children, (child) => {
                if (React.isValidElement<FilterPaneProps>(child)) {
                    return React.cloneElement<FilterPaneProps>(child, {
                        component
                    });
                }
            })}

            {/* Reset */}
            {Object.keys(appliedFilters).length > 0 && (
                <Button type="text" onClick={reset}>
                    Reset
                </Button>
            )}
        </Space>
    );
}
