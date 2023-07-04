import {
    Dropdown,
    Button,
    Checkbox,
    message,
    DatePicker,
    Divider,
    Space
} from 'antd';
import type { MenuProps } from 'antd';
import dayjs from 'dayjs';
import { useState } from 'react';
import { useAppSelector, useAppDispatch } from '@redux';
import { actions } from '@features/log/slice';
import * as logApi from '@features/log/api';
import styled from 'styled-components';

// const data = ['error', 'warn', 'info'];

const mapChoicesToItems = (
    data: string[],
    appliedChoices: string[]
): MenuProps['items'] => {
    return data.map((key) => ({
        key,
        label: (
            <Checkbox
                checked={appliedChoices ? appliedChoices.includes(key) : false}
                value={key}
            >
                {key || 'None'}
            </Checkbox>
        )
    }));
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

interface IProps {
    field: string;
    label: string;
}

export default function DateFilter({ field, label }: IProps) {
    // hooks
    const dispatch = useAppDispatch();
    // store
    const { applied } = useAppSelector((state) => state.log.filtering);
    // state
    const [open, setOpen] = useState(false);
    const [range, setRange] = useState<
        [dayjs.Dayjs | null, dayjs.Dayjs | null]
    >([null, null]);
    // actions

    /**
     * Set dropdown to visible or hidden
     */
    const handleOpenChange = (flag: boolean) => setOpen(flag);

    /**
     * Apply filter and close dropdown.
     */
    const handleFilter = () => {
        const fromTs = range[0]?.utc().format();
        const untilTs = range[1]?.utc().format();

        dispatch(actions.applyFilter({ ...applied, fromTs, untilTs }));
        setOpen(false);
    };

    /**
     * Set respective index of range state when DatePicker date is changed.
     */
    const handleChange = (date: dayjs.Dayjs, item: 'from' | 'until') => {
        const dateUtc = date?.utc();
        if (item === 'from') setRange([dateUtc, range[1]]);
        if (item === 'until') setRange([range[0], dateUtc]);
    };

    const fieldIsApplied =
        ('fromTs' in applied && applied['fromTs']) ||
        ('untilTs' in applied && applied['untilTs']);

    return (
        <>
            <Dropdown
                onOpenChange={handleOpenChange}
                open={open}
                trigger={['click']}
                dropdownRender={() => (
                    <DatePickerDropdown>
                        <Space>
                            <DatePicker
                                format="DD-MMM YY HH:mm:ss"
                                showTime={true}
                                onChange={(date) => handleChange(date, 'from')}
                                presets={[
                                    {
                                        label: '10 min ago',
                                        value: dayjs().subtract(10, 'minutes')
                                    },
                                    {
                                        label: '60 min ago',
                                        value: dayjs().subtract(60, 'minutes')
                                    },
                                    {
                                        label: 'Today',
                                        value: dayjs().subtract(10, 'minutes')
                                    }
                                ]}
                                placeholder="From"
                            />
                            <DatePicker
                                format="DD-MMM YY HH:mm:ss"
                                onChange={(date) => handleChange(date, 'until')}
                                showTime={true}
                                placeholder="Until"
                                value={range[1]}
                            />
                        </Space>
                        <Divider style={{ margin: '10px 0px' }} />
                        <Button
                            size="small"
                            type="primary"
                            onClick={handleFilter}
                        >
                            Apply
                        </Button>
                    </DatePickerDropdown>
                )}
            >
                <Button type={fieldIsApplied ? 'primary' : 'default'}>
                    {label}
                </Button>
            </Dropdown>
        </>
    );
}

const DatePickerDropdown = styled.div`
    padding: 4px;
    list-style-type: none;
    background-color: #ffffff;
    background-clip: padding-box;
    border-radius: 8px;
    outline: none;
    box-shadow: 0 6px 16px 0 rgba(0, 0, 0, 0.08),
        0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
`;

const Drop = () => {
    return <DatePicker.RangePicker showTime={true} />;
};
