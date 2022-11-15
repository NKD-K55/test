<section class="logo-search">
  <div class="container" >
    <div class="col-xs-12 col-sm-12 col-md-3 col-lg-3 logo">
      <a href="<?php echo base_url() ?>"><img src="<?php echo base_url() ?>public/images/logo1.jpg" alt="Logo Construction"></a>
    </div>
    <div class="col-xs-12 col-sm-12 col-md-5 col-lg-5 search">
      <div class="contact-row">
        <div class="phone inline">
          <i class="icon fa fa-phone"></i> (87) 888 888 868
        </div>
        <div class="contact inline">
          <i class="icon fa fa-envelope"></i> sale.miuxinh.2022@gmail.com
        </div>
      </div>
      <form action="search" method="get" role="form">
        <div class="input-search">
          <input type="text" class="form-control" id="search_text" name="search" placeholder="Nhập từ khóa để tìm kiếm...">
          <button>
             
              <i class="fa fa-search"></i>
            </button>
          </div>
        </form>
      </div>
      <div class="col-lg-3 col-md-4 col-sm-4 col-xs-12 hidden-xs" style="padding: 24px;">
       <!-- Cart -->
       <div class="cart_header">
        <div id="link-cart">
        <a href="gio-hang" title="Giỏ hàng">
         <span class="cart_header_icon">
          <img src="<?php echo base_url() ?>public/images/cart2.png" alt="Cart">
        </span>
        <span class="box_text">
          <strong class="cart_header_count">Giỏ hàng <span>(<?php  
           if($this->session->has_userdata('Cart')){
            $cart = unserialize($this->session->userdata('Cart'));
            echo count($cart->sanPham);
            
          }else{
            echo 0;
          }
          ?>)</span></strong>
          <span class="cart_price">
            <?php if($this->session->userdata('Cart')):
              $cart=unserialize($this->session->userdata('Cart'));
              ?>
              <?php echo number_format($cart->tongGia).' VNĐ';?>
              <?php else : ?>
                <p>0 VNĐ</p>
              <?php endif; ?>
            </span>
          </span>
        </a>
        </div>
        <div class="cart_clone_box">
          <div class="cart_box_wrap hidden">
           <div class="cart_item original clearfix">
            <div class="cart_item_image">
            </div>
            <div class="cart_item_info">
             <p class="cart_item_title"><a href="" title=""></a></p>
             <span class="cart_item_quantity"></span>
             <span class="cart_item_price"></span>
             <span class="remove"></span>
           </div>
         </div>
       </div>
     </div>
   </div>
   <!-- End Cart -->
   <!-- Account -->
   <div class="user_login">
     <a href="thong-tin-khach-hang" title="Tài khoản">
      <div class="user_login_icon">
       <img src="<?php echo base_url() ?>public/images/user.png" alt="Cart">
     </div>
     <div class="box_text">
       <strong>Tài khoản</strong>
       <!--<span class="cart_price">Đăng nhập, đăng ký</span>-->
     </div>
   </a>
 </div>
</div>
</div>
</section>

<script>


function updateHeader () {
            var strurl="<?php echo base_url();?>"+`/gio-hang/cap-nhat-san-pham-header`;
            $.ajax({
				url: strurl,
				type: 'GET',
				dataType: 'HTML',
				success: function(data) {
                    $('#link-cart').html(data);
				}
			});
        }

</script>
<!-- <script>
  $(document).ready(function(){

   load_data();
   var strurl="<?php echo base_url();?>"+'/search/quick';
   function load_data(query)
   {
    $.ajax({
      url: strurl,
      method:"POST",
      data:{query:query},
      success:function(data){
        if(data){
          $('#result').html(data);
        }else{
          $('#result').html(data);
        }
      }
    })
  }

  $('#search_text').keyup(function(){
    var search = $(this).val();
    if(search != '')
    {
     load_data(search);
   }
   else
   {
     load_data();
   }
 });
});
</script> -->