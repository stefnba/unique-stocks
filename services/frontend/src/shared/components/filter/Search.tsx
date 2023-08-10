import { Button, Input } from 'antd';
import { useState } from 'react';
import type { ActionCreatorWithNonInferrablePayload } from '@reduxjs/toolkit';
import type { FilterComponents } from '@features/filtering/slice.types';
import { actions as filterActions } from '@features/filtering';
import { useAppSelector, useAppDispatch } from '@redux';

const { Search } = Input;

type SearchFilterProps = {
    field: string;
    component?: FilterComponents;
};

export default function SearchFilter({ field, component }: SearchFilterProps) {
    const dispatch = useAppDispatch();
    const [show, setShow] = useState(true);

    // store
    const appliedFilters = useAppSelector(
        (state) => state.filtering[component]
    );

    const handleSearch = (value: string) => {
        if (value.length === 0) {
            return dispatch(
                filterActions.apply({
                    component,
                    filters: { ...appliedFilters, [field]: null }
                })
            );
        }
        return dispatch(
            filterActions.apply({
                component,
                filters: { ...appliedFilters, [field]: value }
            })
        );
    };

    if (show) {
        return (
            <>
                <Search
                    placeholder="Search"
                    style={{ width: 400 }}
                    loading={false}
                    allowClear
                    onSearch={handleSearch}
                />
            </>
        );
    }
    return <Button onClick={() => setShow(true)}>Search</Button>;
}
