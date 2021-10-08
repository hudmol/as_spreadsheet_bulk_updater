function bulkUpdaterHandleSelectAll(selectAllCheckbox, controlledCheckboxes) {
    selectAllCheckbox.on('click', function () {
        var selectAll = false;

        this.indeterminate = false;

        if ($(this).is(':checked')) {
            selectAll = true;
            $(this).prop('checked', true);
        } else {
            $(this).prop('checked', false);
        }

        controlledCheckboxes.prop('checked', selectAll);
    });

    controlledCheckboxes.on('change', function () {
        updateSelectAll();
    });

    function updateSelectAll() {
        var checkedCount = controlledCheckboxes.filter((idx, c) => $(c).is(':checked')).length;

        selectAllCheckbox.indeterminate = false;

        if (checkedCount === controlledCheckboxes.length) {
            // All are set on
            selectAllCheckbox.prop('checked', true);
        } else if (checkedCount === 0) {
            // All are off
            selectAllCheckbox.prop('checked', false);
        } else {
            selectAllCheckbox.prop('checked', false);
            selectAllCheckbox.indeterminate = true;
        }
    };

    updateSelectAll();
}
