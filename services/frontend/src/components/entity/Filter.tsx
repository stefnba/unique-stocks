import FilterPane from '@sharedComponents/filter/Pane';
import { SelectFilter, SearchFilter } from '@sharedComponents/filter';
import { useAppSelector } from '@redux';

export default function EntityFilter() {
    return (
        <FilterPane component="entity">
            <SearchFilter field="search" />
            <SelectFilter
                showFilter={true}
                choicesApiEndpoint="entity/filter/choices/headquarter_address_country"
                label="Country"
                field="headquarter_address_country"
            />
        </FilterPane>
    );
}
