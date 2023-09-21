import FilterPane from '@sharedComponents/filter/Pane';
import { SelectFilter, SearchFilter } from '@sharedComponents/filter';
import { useAppSelector } from '@redux';

export default function ExchangeSecurityFilter() {
    const filtersApplied = useAppSelector((state) => state.filtering.exchange);

    return (
        <FilterPane component="exchangeSecurity">
            <SearchFilter field="search" />
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
