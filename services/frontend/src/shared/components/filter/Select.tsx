import React, { useState } from 'react';
import {
    Dropdown,
    Button,
    Checkbox,
    message,
    theme,
    Divider,
    Input,
    Spin,
    Typography
} from 'antd';
import type { MenuProps } from 'antd';
import type { ActionCreatorWithNonInferrablePayload } from '@reduxjs/toolkit';

import { useAppSelector, useAppDispatch } from '@redux';

import { api } from '@features/filter';

const { useToken } = theme;
const { Text } = Typography;

type Choices = {
    key: string;
    label?: string;
    count?: number;
}[];

const mapChoicesToItems = (
    data: Choices,
    appliedChoices: string[],
    keyLabelMapping?: Record<string, string | number>
): MenuProps['items'] => {
    return data.map(({ key, label, count }) => {
        const isApplied = appliedChoices
            ? appliedChoices.includes(String(key))
            : false;

        const mappedLabel =
            keyLabelMapping && key in keyLabelMapping
                ? keyLabelMapping[key]
                : undefined;

        return {
            key: key,
            label: (
                <Checkbox checked={isApplied} value={key}>
                    {label || mappedLabel || key || 'None'}
                    {count && (
                        <Text type="secondary">
                            {' '}
                            ({count.toLocaleString('en-US')})
                        </Text>
                    )}
                </Checkbox>
            )
        };
    });
};

const buildFilter = (
    field: string,
    choice: string | number,
    applied: { [key: string]: string | unknown[] }
) => {
    let appliedChoices = applied[field] || [];

    // filter value is string
    if (!Array.isArray(appliedChoices)) {
        appliedChoices = [appliedChoices];
    }

    // remove choice from applied filters
    if (appliedChoices.includes(choice)) {
        return {
            ...applied,
            [field]: appliedChoices.filter((item) => item !== choice)
        };
    }

    // add choice to applied filters
    return {
        ...applied,
        [field]: [...appliedChoices, choice]
    };
};

type SelectFilterProps = {
    field: string;
    label: string;
    choicesApiEndpoint?: string;
    choices?: Choices;
    showFilter?: boolean;
    appliedFilters?: { [key: string]: (string | any)[] };
    applyFilterAction?: ActionCreatorWithNonInferrablePayload;
    keyLabelMapping?: Record<string, string | number>;
};

export default function SelectFilter({
    field,
    label,
    appliedFilters,
    showFilter = false,
    applyFilterAction,
    choicesApiEndpoint,
    choices,
    keyLabelMapping
}: SelectFilterProps) {
    const dispatch = useAppDispatch();
    const { token } = useToken();

    // state
    const [isOpen, setOpen] = useState(false);

    const contentStyle: React.CSSProperties = {
        minWidth: 240,
        backgroundColor: token.colorBgElevated,
        borderRadius: token.borderRadiusLG,
        boxShadow: token.boxShadowSecondary
    };

    const appliedFiltersForField = appliedFilters
        ? appliedFilters[field]
        : null;

    const menuStyle: React.CSSProperties = {
        boxShadow: 'none'
    };

    const onClick: MenuProps['onClick'] = ({ key, domEvent, keyPath }) => {
        domEvent.preventDefault();
        dispatch(
            applyFilterAction(
                buildFilter(field, key === 'tmp-0' ? null : key, appliedFilters)
            )
        );
    };

    // run api call only when choicesApiEndpoint is specified
    const { data: choicesData, isLoading } = api.useFilterGetSelectChoicesQuery(
        choicesApiEndpoint,
        {
            skip: !choicesApiEndpoint
        }
    );

    const fieldIsApplied =
        appliedFiltersForField &&
        (appliedFiltersForField as string[]).length > 0;

    const _choices = choicesApiEndpoint ? choicesData : choices;

    return (
        <>
            <Dropdown
                onOpenChange={(open) => setOpen(open)}
                open={isOpen}
                arrow={false}
                menu={
                    _choices && {
                        items: mapChoicesToItems(
                            _choices,
                            appliedFiltersForField,
                            keyLabelMapping
                        )
                    }
                }
                dropdownRender={(menu) => (
                    <div
                        style={contentStyle}
                        onMouseLeave={() => setOpen(false)}
                    >
                        {showFilter && (
                            <>
                                <Input
                                    className="mt-2 "
                                    allowClear
                                    bordered={false}
                                    placeholder="Filter"
                                />
                                <Divider
                                    className="mb-3"
                                    style={{ margin: 0 }}
                                />
                            </>
                        )}
                        {isLoading && <Spin />}
                        {_choices &&
                            React.cloneElement(menu as React.ReactElement, {
                                style: menuStyle,
                                onClick
                            })}
                        {/* <Divider className="mb-3" style={{ margin: 0 }} />
                        <Button type="text">Reset</Button>
                        <Button type="text">Reset</Button> */}
                    </div>
                )}
            >
                <Button type={fieldIsApplied ? 'primary' : 'default'}>
                    {label}
                </Button>
            </Dropdown>
        </>
    );
}
