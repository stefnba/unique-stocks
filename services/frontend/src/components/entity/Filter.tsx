import FilterPane from '@sharedComponents/filter/Pane';
import { SelectFilter, SearchFilter } from '@sharedComponents/filter';
import { useAppSelector } from '@redux';
import { actions } from '@features/entity';

export default function EntityFilter() {
    const { applied } = useAppSelector((state) => state.entity.filtering);

    return (
        <FilterPane
            appliedFilters={applied}
            applyFilterAction={actions.applyFilter}
        >
            <SearchFilter />
            <SelectFilter
                showFilter={true}
                choicesApiEndpoint="entity/filter/choices/headquarter_address_country"
                label="Country"
                field="headquarter_address_country"
            />
        </FilterPane>
    );
}
