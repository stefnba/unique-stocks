import React, { useEffect } from 'react';
import { Space, Button } from 'antd';
import { useSearchParams } from 'react-router-dom';
import type { ActionCreatorWithNonInferrablePayload } from '@reduxjs/toolkit';
import queryString from 'query-string';

type FilterPaneProps = {
    appliedFilters: object;
    applyFilterAction: ActionCreatorWithNonInferrablePayload;
};

import { useAppDispatch } from '@redux';

export default function FilterPane({
    children,
    appliedFilters,
    applyFilterAction
}: React.PropsWithChildren & FilterPaneProps) {
    const dispatch = useAppDispatch();
    let [searchParams, setSearchParams] = useSearchParams();

    // update url when filter is set
    useEffect(() => {
        if (Object.keys(appliedFilters).length > 0) {
            setSearchParams(
                `?${queryString.stringify(appliedFilters, {
                    // arrayFormat: 'comma'
                })}`
            );
        }
    }, [appliedFilters]);

    // set filter based on url on mount
    useEffect(() => {
        dispatch(
            applyFilterAction(
                queryString.parse(searchParams.toString(), {
                    // arrayFormat: 'comma'
                })
            )
        );
    }, []);

    const reset = () => {
        dispatch(applyFilterAction({}));
        setSearchParams();
    };

    return (
        <Space className="mb-8">
            {/* Filters */}
            {React.Children.map(children, (child) => {
                if (React.isValidElement<FilterPaneProps>(child)) {
                    return React.cloneElement<FilterPaneProps>(child, {
                        appliedFilters,
                        applyFilterAction
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
