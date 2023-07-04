import { Dropdown, Button, Checkbox, message } from 'antd';
import type { MenuProps } from 'antd';

import { useAppSelector, useAppDispatch } from '@redux';
import { actions } from '@features/log/slice';
import * as logApi from '@features/log/api';

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

export default function SelectFilter({ field, label }: IProps) {
    const { applied } = useAppSelector((state) => state.log.filtering);

    const dispatch = useAppDispatch();

    const { data } = logApi.useGetFilterChoicesQuery({
        field,
        filter: applied
    });

    const onClick: MenuProps['onClick'] = ({ key, domEvent, keyPath }) => {
        domEvent.preventDefault();
        dispatch(
            actions.applyFilter(
                buildFilter(field, key === 'tmp-0' ? null : key, applied)
            )
        );
    };

    const fieldIsApplied =
        applied[field] && (applied[field] as string[]).length > 0;

    return (
        <div>
            <Dropdown
                menu={{
                    items: mapChoicesToItems(
                        data?.choices || ['error', 'warn', 'info'],
                        applied[field]
                    ),
                    onClick
                }}
            >
                <Button type={fieldIsApplied ? 'primary' : 'default'}>
                    {label}
                </Button>
            </Dropdown>
        </div>
    );
}
