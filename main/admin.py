from django.contrib import admin

from .models import Account, StoreImage, BannerImage


class StoreImageAdmin(admin.StackedInline):
    model = StoreImage

class BannerImageAdmin(admin.StackedInline):
    model = BannerImage

@admin.register(Account)
class AccountAdmim(admin.ModelAdmin):
    inlines = [StoreImageAdmin, BannerImageAdmin]

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)

    # def save_formset(self, request, form, formset, change):
    #     instance = form.instance
    #     return super().save_formset(request, form, formset, change)