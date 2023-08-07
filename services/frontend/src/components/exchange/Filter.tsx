import FilterPane from '@sharedComponents/filter/Pane';
import { SelectFilter, SearchFilter } from '@sharedComponents/filter';
import { useAppSelector } from '@redux';
import { actions } from '@features/exchange';

export default function SecurityFilter() {
    const { applied } = useAppSelector((state) => state.exchange.filtering);

    return (
        <FilterPane
            appliedFilters={applied}
            applyFilterAction={actions.applyFilter}
        >
            <SearchFilter />
            <SelectFilter
                choicesApiEndpoint="exchange/filter/choices/operating_exchange_id"
                label="Type"
                field="exchange_type"
                keyLabelMapping={{
                    NULL: 'Operating Exchange',
                    NOT_NULL: 'Exchange'
                }}
            />
            <SelectFilter
                choicesApiEndpoint="exchange/filter/choices/source"
                label="Source"
                field="source"
            />
        </FilterPane>
    );
}
