import { Button, Input } from 'antd';
import React, { useEffect, useState } from 'react';
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

    const [searchValue, setSearchValue] = useState(null);

    useEffect(() => {
        if (appliedFilters[field]) {
            setSearchValue(appliedFilters[field]);
        }
    }, [appliedFilters[field]]);

    const handleInput = (e: React.ChangeEvent<HTMLInputElement>) => {
        setSearchValue(e.target.value);
    };

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
                    value={searchValue}
                    placeholder="Search"
                    style={{ width: 400 }}
                    loading={false}
                    allowClear
                    onSearch={handleSearch}
                    onChange={handleInput}
                />
            </>
        );
    }
    return <Button onClick={() => setShow(true)}>Search</Button>;
}
